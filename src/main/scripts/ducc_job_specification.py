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

# Retrieve a DUCC Job Specification from Webserver

import sys
import urllib2

from optparse import OptionParser
from HTMLParser import HTMLParser

row = 0
column = 0

job_spec_provider = 'ducc'
job_spec_key = ''
job_spec_value = ''

class DuccServiceDeploymentsTabHTMLParser(HTMLParser):
    
    def handle_starttag(self, tag, attrs):
        global options, row, column, job_spec_provider, job_spec_key, job_spec_value
        #print("Encountered a start tag:", tag)
        if(tag == 'td'):
            column = column + 1

    def handle_endtag(self, tag):
        global options, row, column, job_spec_provider, job_spec_key, job_spec_value
        if(tag == 'tr'):
            display = True
            if(options.user):
                if(job_spec_provider != 'user'):
                    display = False
            if(display):
                if(options.provider):
                    print job_spec_provider, job_spec_key, job_spec_value
                else:
                    print job_spec_key, job_spec_value
            job_spec_provider = 'ducc'
            job_spec_key=''
            job_spec_value = ''
            column = 0
            row = row + 1
        #print("Encountered an end tag :", tag)

    def handle_data(self, data):
        global options, row, column, job_spec_provider, job_spec_key, job_spec_value
        if(column == 1):
            job_spec_provider = data
        elif(column == 2):
            job_spec_key = data
        else:
            job_spec_value = job_spec_value+data
        #print("Encountered some data  :", str(row), str(column), data)

class DuccServiceStatus():
    
    # parse command line
    def parse_cmdline(self):
        global options
        parser = OptionParser()
        parser.add_option('--scheme', action='store', dest='scheme', default='http', help='default = http')
        parser.add_option('--host', action='store', dest='host', default=None, help='required (no default)')
        parser.add_option('--port', action='store', dest='port', default='42133', help='default = 42133')
        parser.add_option('--id', action='store', dest='id', default=None, help='required (no default)')
        parser.add_option('--provider', action='store_true', dest='provider', default=False, help='display provider (optional)')
        parser.add_option('--user', action='store_true', dest='user', default=False, help='display provider==user entries only (optional)')
        (options, args) = parser.parse_args()
        
        if(options.host == None):
            parser.error("missing --host")
    
        if(options.id == None):
            parser.error("missing --id")
        
    def main(self, argv):
        servlet = '/ducc-servlet/job-specification-data'
        self.parse_cmdline()
        url_string = options.scheme+'://'+options.host+':'+options.port+servlet+'?id='+options.id
        #print url_string
        response = urllib2.urlopen(url_string)
        html = response.read()
        
        parser = DuccServiceDeploymentsTabHTMLParser()
        parser.feed(html)

if __name__ == '__main__':
    instance = DuccServiceStatus()
    instance.main(sys.argv[1:])
