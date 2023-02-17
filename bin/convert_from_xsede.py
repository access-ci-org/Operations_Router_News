#!/usr/bin/env python3
#
#  Retrieves all outages from the https://info.xsede.org/ outages API
#  Processes only the outages that originizated from XSEDE
#  Merges when multiple resources were affected by one outage into a single outage record
#  Storing them in the new ACCESS-CI news repository
#
import argparse
from collections import Counter
from datetime import datetime, timezone, tzinfo, timedelta
from django.utils.dateparse import parse_datetime
import http.client as httplib
import json
import logging
import logging.handlers
import os
from pid import PidFile
import pwd
import re
import sys
import shutil
import signal
import ssl
from time import sleep
import traceback
from urllib.parse import urlparse
import pytz
Central_TZ = pytz.timezone('US/Central')

import django
django.setup()
from django.db import DataError, IntegrityError
from django.conf import settings
from django.utils.dateparse import parse_datetime
from news.models import *
from warehouse_state.process import ProcessingActivity

import pdb

# Used during initialization before loggin is enabled
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

class Router():
    def __init__(self):
        parser = argparse.ArgumentParser(epilog='File SRC|DEST syntax: file:<file path and name')
        parser.add_argument('daemonaction', nargs='?', choices=('start', 'stop', 'restart'), \
                            help='{start, stop, restart} daemon')
        parser.add_argument('-s', '--source', action='store', dest='src', \
                            help='Messages source {file, http[s]} (default=file)')
        parser.add_argument('-d', '--destination', action='store', dest='dest', \
                            help='Message destination {file, analyze, or warehouse} (default=analyze)')
        parser.add_argument('--daemon', action='store_true', \
                            help='Daemonize execution')
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', default='./convert_from_xsede.conf', \
                            help='Configuration file default=./convert_from_xsede.conf')
        parser.add_argument('--pdb', action='store_true', \
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        # Trace for debugging as early as possible
        if self.args.pdb:
            pdb.set_trace()

        # Load configuration file
        self.config_file = os.path.abspath(self.args.config)
        try:
            with open(self.config_file, 'r') as file:
                conf=file.read()
        except IOError as e:
            eprint('Error "{}" reading config={}'.format(e, config_path))
            sys.exit(1)
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            eprint('Error "{}" parsing config={}'.format(e, config_path))
            sys.exit(1)

        if self.config.get('PID_FILE'):
            self.pidfile_path =  self.config['PID_FILE']
        else:
            name = os.path.basename(__file__).replace('.py', '')
            self.pidfile_path = '/var/run/{}/{}.pid'.format(name, name)

    def Setup(self, peak_sleep=10, offpeak_sleep=60, max_stale=24 * 60):
        # Initialize log level from arguments, or config file, or default to WARNING
        loglevel_str = (self.args.log or self.config.get('LOG_LEVEL', 'WARNING')).upper()
        loglevel_num = getattr(logging, loglevel_str, None)
        self.logger = logging.getLogger('DaemonLog')
        self.logger.setLevel(loglevel_num)
        self.formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)s %(message)s', \
                                           datefmt='%Y/%m/%d %H:%M:%S')
        self.handler = logging.handlers.TimedRotatingFileHandler(self.config['LOG_FILE'], \
            when='W6', backupCount=999, utc=True)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        # Initialize stdout, stderr
        if self.args.daemon and 'LOG_FILE' in self.config:
            self.stdout_path = self.config['LOG_FILE'].replace('.log', '.daemon.log')
            self.stderr_path = self.stdout_path
            self.SaveDaemonStdOut(self.stdout_path)
            sys.stdout = open(self.stdout_path, 'wt+')
            sys.stderr = open(self.stderr_path, 'wt+')

        signal.signal(signal.SIGINT, self.exit_signal)
        signal.signal(signal.SIGTERM, self.exit_signal)

        self.logger.info('Starting program=%s pid=%s, uid=%s(%s)' % \
                     (os.path.basename(__file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))

        self.src = {}
        self.dest = {}
        for var in ['uri', 'scheme', 'path', 'display']: # Where <full> contains <type>:<obj>
            self.src[var] = None
            self.dest[var] = None
        self.peak_sleep = peak_sleep * 60       # 10 minutes in seconds during peak business hours
        self.offpeak_sleep = offpeak_sleep * 60 # 60 minutes in seconds during off hours
        self.max_stale = max_stale * 60         # 24 hours in seconds force refresh
        default_file = 'file:./news.json'

        # Verify arguments and parse compound arguments
        if not getattr(self.args, 'src', None): # Tests for None and empty ''
            if 'SOURCE_URL' in self.config:
                self.args.src = self.config['SOURCE_URL']
        if not getattr(self.args, 'src', None): # Tests for None and empty ''
            self.args.src = default_file
        idx = self.args.src.find(':')
        if idx > 0:
            (self.src['scheme'], self.src['path']) = (self.args.src[0:idx], self.args.src[idx+1:])
        else:
            (self.src['scheme'], self.src['path']) = (self.args.src, None)
        if self.src['scheme'] not in ['file', 'http', 'https']:
            self.logger.error('Source not {file, http, https}')
            sys.exit(1)
        if self.src['scheme'] in ['http', 'https']:
            if self.src['path'][0:2] != '//':
                self.logger.error('Source URL not followed by "//"')
                sys.exit(1)
            self.src['path'] = self.src['path'][2:]
        self.src['uri'] = self.args.src
        self.src['display'] = self.args.src

        if not getattr(self.args, 'dest', None): # Tests for None and empty ''
            if 'DESTINATION' in self.config:
                self.args.dest = self.config['DESTINATION']
        if not getattr(self.args, 'dest', None): # Tests for None and empty ''
            self.args.dest = 'analyze'
        idx = self.args.dest.find(':')
        if idx > 0:
            (self.dest['scheme'], self.dest['path']) = (self.args.dest[0:idx], self.args.dest[idx+1:])
        else:
            self.dest['scheme'] = self.args.dest
        if self.dest['scheme'] not in ['file', 'analyze', 'warehouse']:
            self.logger.error('Destination not {file, analyze, warehouse}')
            sys.exit(1)
        self.dest['uri'] = self.args.dest
        if self.dest['scheme'] == 'warehouse':
            self.dest['display'] = '{}@database={}'.format(self.dest['scheme'], settings.DATABASES['default']['HOST'])
        else:
            self.dest['display'] = self.args.dest

        if self.src['scheme'] in ['file'] and self.dest['scheme'] in ['file']:
            self.logger.error('Source and Destination can not both be a {file}')
            sys.exit(1)

        self.ORGANIZATIONID = self.config.get('ORGANIZATIONID', 'NONE')  # Default to NONE
        if not self.ORGANIZATIONID:
            self.logger.error('Required ORGANIZATIONID configuration is missing')
            sys.exit(1)
        try:
            self.PUBLISHER = News_Publisher.objects.get(pk=self.ORGANIZATIONID)
        except News_Publisher.DoesNotExist:
            self.logger.error('Configured ORGANIZATIONID is not a registered News Publisher')
            sys.exit(1)

        self.AFFILIATION = self.config.get('AFFILIATION', 'NONE')  # Default to NONE
        self.NEWSURNPREFIX = self.PUBLISHER.NewsURNPrefix or self.config.get('NEWSURNPREFIX', ['NONE'])  # Default to NONE
        self.INPUTURNPREFIX = self.config.get('INPUTURNPREFIX', 'NONE')  # Default to NONE
        self.PUBLISHERNAME = self.PUBLISHER.OrganizationName
        
        if self.args.daemonaction == 'start':
            if self.src['scheme'] not in ['http', 'https'] or self.dest['scheme'] not in ['warehouse']:
                self.logger.error('Can only daemonize when source=[http|https] and destination=warehouse')
                sys.exit(1)

        self.logger.info('Source: ' + self.src['display'])
        self.logger.info('Destination: ' + self.dest['display'])
        self.logger.info('Config: ' + self.config_file)
        self.logger.info('Affiliation: ' + self.AFFILIATION)
        self.logger.info('Publisher: {} ({})'.format(self.PUBLISHERNAME, self.ORGANIZATIONID))
        self.logger.info('NewsURNPrefix: ' + self.NEWSURNPREFIX)
        self.logger.info('InputURNPrefix: ' + self.INPUTURNPREFIX)

    def SaveDaemonStdOut(self, path):
        # Save daemon log file using timestamp only if it has anything unexpected in it
        try:
            file = open(path, 'r')
            lines = file.read()
            file.close()
            if not re.match("^started with pid \d+$", lines) and not re.match("^$", lines):
                ts = datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')
                newpath = '{}.{}'.format(path, ts)
                self.logger.debug('Saving previous daemon stdout to {}'.format(newpath))
                shutil.copy(path, newpath)
        except Exception as e:
            self.logger.error('Exception in SaveDaemonStdOut({})'.format(path))
        return

    def exit_signal(self, signum, frame):
        self.logger.critical('Caught signal={}({}), exiting with rc={}'.format(signum, signal.Signals(signum).name, signum))
        sys.exit(signum)

    def exit(self, rc):
        if rc:
            self.logger.error('Exiting with rc={}'.format(rc))
        sys.exit(rc)

    def Read_SOURCE(self, url):
        urlp = urlparse(url)
        if not urlp.scheme or not urlp.netloc or not urlp.path:
            self.logger.error('SOURCE URL is not valid: {}'.format(url))
            sys.exit(1)
        if urlp.scheme not in ['http', 'https']:
            self.logger.error('SOURCE URL scheme is not valid: {}'.format(url))
            sys.exit(1)
        if ':' in urlp.netloc:
            (host, port) = urlp.netloc.split(':')
        else:
            (host, port) = (urlp.netloc, '')
        if not port:
            port = '80' if urlp.scheme == 'http' else '443'     # Default is HTTPS/443
        
        headers = {'Content-type': 'application/json'
            }
#        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
#   2023-02-16 JP - figure out later the appropriate level of ssl verification
        ctx = ssl._create_unverified_context()
        conn = httplib.HTTPSConnection(host=host, port=port, context=ctx)
        conn.request('GET', urlp.path, None , headers)
        self.logger.debug('HTTP GET  {}'.format(url))
        response = conn.getresponse()
        result = response.read()
        self.logger.debug('HTTP RESP {} {} (returned {}/bytes)'.format(response.status, response.reason, len(result)))
        try:
            input_json = json.loads(result)
        except ValueError as e:
            self.logger.error('Response not in expected JSON format ({})'.format(e))
            return(None)
        return(input_json)

    def Analyze_SOURCE(self, input_obj):
        maxlen = {}
        for p_res in input_obj:  # Parent resources
            self.STATS.update({'Update'})
            self.logger.info('Item={}, Affected={}, StartDatetime="{}"'.format(p_res['view_node'], len(p_res['affected_infrastructure_elements']), p_res['start_timestamp'], p_res['resource_descriptive_name']))

    def Write_Cache(self, file, input_obj):
        data = json.dumps(input_obj)
        with open(file, 'w') as my_file:
            my_file.write(data)
            my_file.close()
        self.logger.info('Serialized and wrote {} bytes to file={}'.format(len(data), file))
        return(len(data))

    def Read_Cache(self, file):
        with open(file, 'r') as my_file:
            data = my_file.read()
            my_file.close()
        try:
            input_obj = json.loads(data)
            self.logger.info('Read and parsed {} bytes from file={}'.format(len(data), file))
            return(input_obj)
        except ValueError as e:
            self.logger.error('Error "{}" parsing file={}'.format(e, file))
            sys.exit(1)

    def Warehouse_XSEDE_News(self, input_obj):
        self.cur = {}   # Current items
        self.new = {}   # New items
        for item in News.objects.filter(URN__startswith=self.NEWSURNPREFIX):
            self.cur[item.URN] = item

        self.cur_assoc = {} # Current associations
        self.new_assoc = {} # New associations
        for item in News_Associations.objects.filter(NewsItem__URN__startswith=self.NEWSURNPREFIX):
            KEY = str(item)
            self.cur_assoc[KEY] = item

        # Merges an outage for multiple resources into one
        merged_obj = {}
        for p_res in input_obj:
            # Filter out new ACCESS outages and processes only legacy XSEDE ones
            if not p_res['ID'].startswith(self.INPUTURNPREFIX):
                continue
            URN = '{}{}'.format(self.NEWSURNPREFIX, p_res['OutageID'])
            RESOURCE = p_res['ResourceID']
            if URN not in merged_obj:
                merged_obj[URN] = p_res
                merged_obj[URN].pop('ResourceID', None)
                merged_obj[URN]['AffectedInfrastructure'] = []
            merged_obj[URN]['AffectedInfrastructure'].append(RESOURCE)
            
        for (URN, p_res) in merged_obj.items():
            if p_res['OutageType'] == 'Reconfiguration':
                TYPE = 'Reconfiguration'
            elif p_res['OutageType'] == 'Partial':
                TYPE = 'Outage Partial'
            else:
                TYPE = 'Outage Full'
            WEBURL = 'https://operations-api.access-ci.org/wh2/news/v1/id/{}/?format=html'.format(URN)
            try:
                news_item, created = News.objects.update_or_create(
                                    URN = URN,
                                    defaults = {
                                        'Subject': p_res['Subject'],
                                        'Content': p_res['Content'],
                                        'NewsStart': parse_datetime(p_res['OutageStart']),
                                        'NewsEnd': parse_datetime(p_res['OutageEnd']),
                                        'NewsType': TYPE,
                                        'DistributionOptions': None,
                                        'WebURL': WEBURL,
                                        'Affiliation': self.AFFILIATION,
                                        'Publisher':  self.PUBLISHER
                                     })
                news_item.save()
                self.new[URN] = news_item
                self.STATS.update({'Update'})
                self.logger.debug('News URN={}'.format(URN))
            except (DataError, IntegrityError) as e:
                msg = '{} saving News URN={} ({}): {}'.format(type(e).__name__, URN, e.message)
                self.logger.error(msg)
                return(False, msg)
        
            for resource in p_res['AffectedInfrastructure']:
                try:
                    assoc_type = 'Resource'
                    assoc_id = resource
                    KEY = '{}->{}/{}'.format(URN, assoc_type, assoc_id) # Identical to str(News_Association item)
                    if KEY in self.cur_assoc:   # Already exists
                        self.new_assoc[KEY] = self.cur_assoc[KEY]
                        continue
                    associated_item = News_Associations(
                        NewsItem = news_item,
                        AssociatedType = assoc_type,
                        AssociatedID = assoc_id)
                    associated_item.save()
                    self.new_assoc[KEY]=associated_item
                    self.logger.debug('Assoc KEY={}'.format(KEY))
                except (DataError, IntegrityError) as e:
                    msg = '{} saving Assoc KEY={} ({}): {}'.format(type(e).__name__, KEY, e.message)
                    self.logger.error(msg)
                    return(False, msg)

        # Delete obsolete associations
        for KEY in self.cur_assoc:
            if KEY not in self.new_assoc:
                try:
                    News_Associations.objects.get(pk=self.cur_assoc[KEY].id).delete()
                    self.logger.info('Deleted Assoc KEY={}'.format(KEY))
                except (DataError, IntegrityError) as e:
                    self.logger.error('{} deleting Assoc KEY={}: {}'.format(type(e).__name__, KEY, e.message))

        # Delete obsolete new items
        for URN in self.cur:
            if URN not in self.new:
                try:
                    News.objects.filter(URN=URN).delete()
                    self.STATS.update({'Delete'})
                    self.logger.info('Deleted News URN={}'.format(URN))
                except (DataError, IntegrityError) as e:
                    self.logger.error('{} deleting News URN={}: {}'.format(type(e).__name__, URN, e.message))
        return(True, '')
            
    def smart_sleep(self, last_run):
        # Between 6 AM and 9 PM Central
        current_sleep = self.peak_sleep if 6 <= datetime.now(Central_TZ).hour <= 21 else self.offpeak_sleep
        self.logger.debug('sleep({})'.format(current_sleep))
        sleep(current_sleep)

    def Run(self):
        while True:
            self.start = datetime.now(timezone.utc)
            self.STATS = Counter()
            
            if self.src['scheme'] == 'file':
                SOURCE_DATA = self.Read_Cache(self.src['path'])
            else:
                SOURCE_DATA = self.Read_SOURCE(self.src['uri'])

            if SOURCE_DATA:
                if self.dest['scheme'] == 'file':
                    bytes = self.Write_Cache(self.dest['path'], SOURCE_DATA)
                elif self.dest['scheme'] == 'analyze':
                    self.Analyze_SOURCE(SOURCE_DATA)
                elif self.dest['scheme'] == 'warehouse':
                    pa_application=os.path.basename(__file__)
                    pa_function='Warehouse_XSEDE_News'
                    pa_topic = 'Infrastructure News'
                    pa_about = self.AFFILIATION
                    pa_id = '{}:{}:{}'.format(pa_application, pa_function, pa_topic)
                    pa = ProcessingActivity(pa_application, pa_function, pa_id , pa_topic, pa_about)
                    (rc, warehouse_msg) = self.Warehouse_XSEDE_News(SOURCE_DATA)
                
                self.end = datetime.now(timezone.utc)
                summary_msg = 'Processed in {:.3f}/seconds: {}/updates, {}/deletes, {}/skipped'.format((self.end - self.start).total_seconds(), self.STATS['Update'], self.STATS['Delete'], self.STATS['Skip'])
                self.logger.info(summary_msg)
                if self.dest['scheme'] == 'warehouse':
                    if rc:  # No errors
                        pa.FinishActivity(rc, summary_msg)
                    else:   # Something failed, use returned message
                        pa.FinishActivity(rc, warehouse_msg)
            if not self.args.daemonaction:
                break
            self.smart_sleep(self.start)

########## CUSTOMIZATIONS END ##########

if __name__ == '__main__':
    router = Router()
    with PidFile(router.pidfile_path):
        try:
            router.Setup()
            rc = router.Run()
        except Exception as e:
            msg = '{} Exception: {}'.format(type(e).__name__, e)
            router.logger.error(msg)
            traceback.print_exc(file=sys.stdout)
            rc = 1
    router.exit(rc)
