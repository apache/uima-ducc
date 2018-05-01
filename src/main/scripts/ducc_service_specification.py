#!/usr/bin/env python

# -----------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# -----------------------------------------------------------------------

# Retrieve a DUCC Service Specification from Webserver

import sys
import urllib2

from optparse import OptionParser
from HTMLParser import HTMLParser

message = 'Python 2.7 or greater is required.'
sysinfo = sys.version_info
if(sysinfo[0] < 2):
    print message
    sys.exit(1)
elif(sysinfo[1] < 7):
    print message
    sys.exit(1)

row = 0
column = 0

service_spec_provider = 'ducc'
service_spec_key = ''
service_spec_value = ''

service_spec_instances = ''

class DuccServiceDeploymentsTabHTMLParser(HTMLParser):
    
    def handle_starttag(self, tag, attrs):
        global options, row, column, service_spec_provider, service_spec_key, service_spec_value, service_spec_attrs
        #print('Encountered a start tag:', tag)
        if(tag == 'td'):
            column = column + 1
        service_spec_attrs = attrs
        
    def handle_endtag(self, tag):
        global options, row, column, service_spec_provider, service_spec_key, service_spec_value, service_spec_instances
        if(tag == 'tr'):
            # we calculate instances from number of implementors
            if(service_spec_key == 'implementors'):
                try:
                    service_spec_instances = len(service_spec_value.split())
                except:
                    pass
            # we set instances from previously calculated number of implementors
            if(service_spec_key == 'instances'):
                service_spec_value = service_spec_instances
            # display row
            display = True
            if(options.user):
                if(service_spec_provider != 'user'):
                    display = False
            if(options.nocp):
                if(service_spec_key == 'service_ping_classpath'):
                    display = False
            if(options.noargs):
                if(service_spec_key == 'process_executable_args'):
                    display = False
            if(display):
                if(options.provider):
                    print service_spec_provider, service_spec_key, service_spec_value
                else:
                    print service_spec_key, service_spec_value
            # set up for next row
            service_spec_provider = 'ducc'
            service_spec_key = ''
            service_spec_value = ''
            column = 0
            row = row + 1
        #print('Encountered an end tag :', tag)

    def handle_data(self, data):
        global options, row, column, service_spec_provider, service_spec_key, service_spec_value, service_spec_attrs
        if(column == 1):
            service_spec_provider = data
        elif(column == 2):
            service_spec_key = data
        else:
            append = True
            if(service_spec_key == 'autostart'):
                if(len(service_spec_attrs) < 2):
                    append = False   
            if(append):
                service_spec_value = service_spec_value+' '+data
            
        #print('Encountered some data  :', str(row), str(column), data)

class DuccServiceStatus():
    
    # parse command line
    def parse_cmdline(self):
        global options
        parser = OptionParser()
        parser.add_option('--scheme', action='store', dest='scheme', default='http', help='default = http')
        parser.add_option('--host', action='store', dest='host', default=None, help='required (no default)')
        parser.add_option('--port', action='store', dest='port', default='42133', help='default = 42133')
        parser.add_option('--name', action='store', dest='name', default=None, help='required (no default)')
        parser.add_option('--provider', action='store_true', dest='provider', default=False, help='display provider (optional)')
        parser.add_option('--user', action='store_true', dest='user', default=False, help='display provider==user entries only (optional)')
        parser.add_option('--nocp', action='store_true', dest='nocp', default=False, help='suppress display of service_ping_classpath (optional)')
        parser.add_option('--noargs', action='store_true', dest='noargs', default=False, help='suppress display process_executable_args (optional)')
        (options, args) = parser.parse_args()
        
        if(options.host == None):
            parser.error('missing --host')
    
        if(options.name == None):
            parser.error('missing --name')
        
    def main(self, argv):
        servlet = '/ducc-servlet/service-registry-data'
        self.parse_cmdline()
        url_string = options.scheme+'://'+options.host+':'+options.port+servlet+'?name='+options.name
        #print url_string
        response = urllib2.urlopen(url_string)
        html = response.read()
        
        parser = DuccServiceDeploymentsTabHTMLParser()
        parser.feed(html)

if __name__ == '__main__':
    instance = DuccServiceStatus()
    instance.main(sys.argv[1:])
