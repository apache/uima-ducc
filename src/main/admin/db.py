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

import os
import sys
import getopt
import httplib
import base64
import json
import textwrap

from ducc_util import DuccUtil
from properties  import Properties
import database as db

class DbUtil(DuccUtil):

    def __init__(self):
        DuccUtil.__init__(self)

    def pprint(self, j):
        print json.dumps(j, sort_keys=True, indent=4, separators=(',',':'))
        

    #"clientId":"-",
    #"commandDetail":"-",
    #"commandInfo":"Listening",
    #"connectedOn":"2015-08-05 06:21:30",
    #"connectionId":"12",
    #"db":"DuccHistory",
    #"driver":"OrientDB Java v2.1-rc6 Protocol v30",
    #"lastCommandDetail":"SELECT * FROM VRESERVATION WHERE ducc_dbid=944",
    #"lastCommandInfo":"Execute remote command",
    #"lastCommandOn":"2015-08-10 12:36:11",
    #"lastExecutionTime":"3",
    #"protocol":"binary",
    #"remoteAddress":"/192.168.2.196:48994",
    #"totalRequests":"39171",
    #"totalWorkingTime":"196975",
    #"user":"admin"

    def format_connections(self, j):
        keys = j[0].keys()
        slen = {}
        for k in keys:
            maxv = len(k)
            print 'initial', maxv
            for c in j:
                maxv = max(maxv,  len(c[k]))
            slen[k] = maxv

        print slen
        format = ''
        for k in keys:
            format = format + '%' + str(slen[k]) + 's | '


        tmp = j[0]
        self.pprint(j[0])
        print format               
        print tmp
        print len(tmp)
        print format % (tuple(tmp.keys()))
        print format % (tuple(tmp.values()))
        return

        i = 1
        for c in j:        # connectins in json
            print 'Connection', i
            i = i + 1
            self.pprint(c)

            maxl = 0
            for k in c.keys():
                maxl = max(maxl, len(k))
            print 'max:', maxl
            
        
    def show_connections(self):
        print 'Show connections'
        
        pw = base64.b64encode('root:asdfasdf')

        try:
            conn = httplib.HTTPConnection(self.dbrest)
            conn.request('GET', '/server', None, {'Authorization':'Basic ' + pw})
        except Exception, (e):
            print "    Checking connection: ", e
            return False

        
        resp = conn.getresponse()
        #print 'response code', resp.status, resp.reason
        data = resp.read()
        #print 'Data:', data
        
        if ( resp.status == 200 ):
            #print data
            conns = json.loads(data)
            #print json.dumps(conns, sort_keys=True, indent=4, separators=(',',':'))

            #self.pprint(conns['connections'])
            #print conns['dbs']
            #print conns['properties']
            #print conns['storages']
            print 'Connections:', len(conns['connections'])
            self.format_connections(conns['connections'])


    #
    # Open an OrientDb console on the database defined for this DUCC instance
    #
    def open_console(self):
            
        main = 'com.orientechnologies.orient.graph.console.OGremlinConsole'

        jp = ''
        for k in self.jvm_parms.keys():
            v = self.jvm_parms[k]
            if ( v == None ):
                jp = jp + k + ' '
            else:
                jp = jp + k + '=' + v + ' '

        cmd = ' '.join([self.java(), jp, '-cp', self.classpath, main, ])

        here = os.getcwd()
        os.chdir(self.db_rt)
        pid = self.spawn(cmd)
        
    def usage(self, *msg):
        if ( msg[0] != None ):
            print ' '.join(msg)

        print 'Usage:'
        print '   db.py [options]'
        print ''
        print 'Where options include:'
        print '   -c     show connection status'
        print ''
        print 'If no options are given a console to the database is opened.'
        sys.exit(0)


    def main(self, argv):

        # The dom gets loaded by the import above - or not
        if ( not db.domloaded ):            
            print "Unable to read database configuration; insure the installed Python supports xml.dom.minidom"
            print "Note that Python must be at least version 2.6 but not 3.x.  You are running version", sys.version_info
            return

        (self.jvm_parms, self.classpath, self.db_rt, self.dburl, self.dbrest, self.dbroot) = self.db_parms
        try:
           opts, args = getopt.getopt(argv, 'ch?', ['--connections', '--help'])
        except:
            self.usage('Bad arguments ' + ' '.join(argv))
    

        if ( len(opts) == 0 ):
            self.open_console()
            return

        for ( o, a ) in opts:
            print 'o', o, 'a', a
            if ( o in ['-c', '--connections']):
                self.show_connections()
            elif ( o in ['-h', '-?', '--help'] ):
                self.usage()


if __name__ == "__main__":
    ducc = DbUtil()
    ducc.main(sys.argv[1:])
