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
import os.path
import sys
import getopt
import time

from ducc_util import DuccUtil
from local_hooks import verify_slave_node

from properties import Properties
from properties import Property

class Ducc(DuccUtil):


    def run_db(self):
        
        print '-------- starting the database'
        if ( self.db_bypass ):
            print 'Database is disabled; not starting it.'
            print 'OK'
            return 

        # check for the pid to see if the DB is running.
        if ( self.db_process_alive() ) :
            print 'Database is already running.'
            print 'OK'
            return

        here = os.getcwd()
        self.verify_limits()

        xmx = self.ducc_properties.get('ducc.database.mem.heap')
        new = self.ducc_properties.get('ducc.database.mem.new')

        if ( not ( xmx == None and new == None ) ):   # if either is set
            if ( xmx == None or new == None ) :      # then both must be set
                print "Database properties ducc.database.mem.heap and ducc.database.mem.new must both be set."
                print 'NOTOK'
                return            
            os.environ['MAX_HEAP_SIZE'] = xmx
            os.environ['HEAP_NEWSIZE'] = new

        jmxport = self.ducc_properties.get('ducc.database.jmx.port')
        if ( jmxport != None ):
            os.environ['JMX_PORT'] = jmxport

        jmxhost = self.ducc_properties.get('ducc.database.jmx.host')
        if ( jmxhost != None and jmxhost != 'localhost' ):
            os.environ['LOCAL_JMX'] = 'no'

        os.chdir(self.DUCC_HOME + "/cassandra-server")
        CMD = "bin/cassandra -p " + self.db_pidfile + " > " + self.db_logfile + " 2>&1"
        print '------- Running', CMD

        os.system(CMD);
        print "Database is started.  Waiting for initialization";

        # DB (cassandra) starts and take a moment before anything works.  DbAlive retries for a while
        # to (a) connect as (b) ducc with (c) initialized database
        if ( self.db_alive() ):
            print "OK"
        else:
            # The database MIGHT be started but not configured or otherwise faulty.  db_alive() prints
            # some useful hints.  we indicate failure here though so you don't proceed too far before
            # fixing it.
            print "NOTOK"

    def run_broker(self, simtest):
        broker_port = self.ducc_properties.get('ducc.broker.port')
        broker_jmx_port = self.ducc_properties.get('ducc.broker.jmx.port')
        broker_url_decoration = self.ducc_properties.get('ducc.broker.server.url.decoration')
        broker_memory_opts = self.ducc_properties.get('ducc.broker.memory.options')
        broker_config = self.ducc_properties.get('ducc.broker.configuration')
        broker_home = self.ducc_properties.get('ducc.broker.home')
        broker_credentials = self.ducc_properties.get('ducc.broker.credentials.file')
        if ( simtest ):
            broker_config = 'conf/activemq-ducc-unsecure.xml'

        if ( broker_config[0] != '/' ):     # relative to broker_home if not absolute
            broker_config = broker_home + '/' + broker_config

        os.environ['ACTIVEMQ_OPTS'] = '-DDUCC_AMQ_PORT=' + broker_port + \
            ' -DDUCC_AMQ_JMX_PORT=' + broker_jmx_port + \
            ' -DDUCC_AMQ_DECORATION=' + broker_url_decoration + \
            ' -DDUCC_BROKER_CREDENTIALS_FILE=' + broker_credentials + \
            ' ' + broker_memory_opts
        os.environ['ACTIVEMQ_HOME'] = broker_home

        print 'ACTIVEMQ_OPTS:', os.environ['ACTIVEMQ_OPTS']
        print 'ACTIVEMQ_HOME:', os.environ['ACTIVEMQ_HOME']
        
        self.verify_limits()
        
        here = os.getcwd()
        os.chdir(broker_home + '/bin')
        CMD = './activemq start xbean:' + broker_config 
        self.spawn(CMD)
        os.chdir(here)
        
        print "Started AMQ broker"

    def add_to_classpath(self, lib):
        os.environ['CLASSPATH'] = os.environ['CLASSPATH'] + ":" + lib

    def prepend_classpath(self, lib):
        os.environ['CLASSPATH'] = lib + ":" + os.environ['CLASSPATH'] 

    def run_component(self, component, or_parms, numagents, rmoverride, background, nodup, localdate):

        if ( component == 'all' ):
            component = 'rm,sm,pm,ws,orchestrator'
    	if ( component == 'head' ):
            component = 'rm,sm,pm,ws,orchestrator'
            
        complist = component.split(',')
        args = None

        # ducc-head needs to be in system properties before the ducc daemon reads ducc.properties
        # to insure it can be substituted properly
        ducc_head = self.ducc_properties.get('ducc.head')
        ducc_home = self.DUCC_HOME
        CLASSPATH = os.environ['CLASSPATH']

        jvm_opts = []
        jvm_opts.append('-Dos.page.size=' + self.os_pagesize)
        jvm_opts.append('-Dducc.deploy.configuration=' + self.DUCC_HOME + '/resources/ducc.properties')
        jvm_opts.append('-Dducc.head=' + ducc_head)
        jvm_opts.append('-Dlog4j.configuration=file://' + self.DUCC_HOME + '/resources/log4j.xml')

        service = 'org.apache.uima.ducc.common.main.DuccService'
        for c in complist:
            if ( c == 'agent' ):
                if ( len(complist) > 1 ):
                    print "Must start agents separately"
                    sys.exit(1)
                    
                if ( not self.verify_jvm() ):
                    return

                if ( not self.check_clock_skew(localdate) ):
                    return

                if ( not self.verify_limits() ):
                    return

                (viable, elevated, safe) = self.verify_duccling()
                if ( not self.duccling_ok(viable, elevated, safe) ): 
                    print 'NOT_OK Cannot proceed because of ducc_ling problems.'
                    return

                if ( not verify_slave_node(localdate, self.ducc_properties) ):
                    # we assume that verify_local_node is spewing a line of the form
                    #    NOTOK error message
                    # if all is not fine
                    print '0 ONE RETURNS'

                    return

                jvm_opts.append('-Djava.library.path=' + self.DUCC_HOME) 
                if ( self.agent_jvm_args != None ):
                    jvm_opts.append(self.agent_jvm_args)

                if ( (numagents > 1) ):
                    print '-------------------- launching special agent --------------------'
                    service = 'org.apache.uima.ducc.agent.launcher.Launcher'
                    args = ' ' + str(numagents)
                    jvm_opts.append('-DIP=192.168.3.85') 
                else:
                    ducc_component = '-Dducc.deploy.components=agent'

            if ( c == 'rm' ):
                if ( int(rmoverride) > 0 ):
                    jvm_opts.append("-Dducc.rm.override.dram=" + rmoverride)
                if ( self.rm_jvm_args != None ):
                    jvm_opts.append(self.rm_jvm_args)
                
            if ( c == 'ws' ):
                here = os.getcwd()
                os.chdir(self.DUCC_HOME + '/webserver')
                if ( self.ws_jvm_args != None ):
                    jvm_opts.append(self.ws_jvm_args)
                self.add_to_classpath(ducc_home + '/webserver/lib/*')
                self.add_to_classpath(ducc_home + '/webserver/lib/apache-jsp/*')
                self.add_to_classpath(ducc_home + '/webserver/lib/apache-jstl/*')
                self.add_to_classpath(ducc_home + '/webserver/lib/websocket/*')
                
            if ( c == 'orchestrator' ):
                if ( or_parms != None ):
                    args = '-' + or_parms
                if ( self.or_jvm_args != None ):
                    jvm_opts.append(self.or_jvm_args)
                self.add_to_classpath(ducc_home + '/webserver/lib/*')       
                self.add_to_classpath(ducc_home + '/lib/uima-ducc/user/*')

            if ( c == 'pm' ):
                if ( self.pm_jvm_args != None ):
                    jvm_opts.append(self.pm_jvm_args)
                                        
            if ( c == 'sm' ):
                if ( self.sm_jvm_args != None ):
                    jvm_opts.append(self.sm_jvm_args)
                self.add_to_classpath(ducc_home + '/webserver/lib/*')       
                self.add_to_classpath(ducc_home + '/lib/uima-ducc/user/*')

        if (component != 'agent'):
            service = 'org.apache.uima.ducc.common.main.DuccService'
            ducc_component = '-Dducc.deploy.components=' + component

        # check to see if there is a process like this running already, and barf if so
        # usually called with --nodup, but the sim needs multiple agents yes on the node
        pid = None
        if ( nodup ):
            response = self.find_ducc_process(self.localhost)
            if ( response[0] ):    # something is returned
                proclist = response[1]
                for proc in proclist:
                    r_component  = proc[0]
                    r_pid        = proc[1]
                    r_found_user = proc[2]
                    if ( r_found_user != os.environ['LOGNAME'] ):   # don't care about other stuff
                        continue
                    if ( r_component == component ):
                        print "WARN Not starting", component + ': already running in PID', r_found_user, r_pid, 'on node', self.localhost
                        return

        # not already running, and the node is viable.  fire it off.
        if ( not self.verify_limits() ):
            return
        cmd = []
        cmd.append(self.java())
        cmd.append(ducc_component)
        cmd = cmd + jvm_opts
        cmd.append(service)

        if ( args != None ):
            cmd.append(args)

        #print 'CMD', cmd

        if ( pid == None ):
            if ( background ):
                pid = self.nohup(cmd)
            else:
                pid = self.spawn(' '.join(cmd))
            print 'PID ' + str(pid)

        if ( c == 'ws' ):
            os.chdir(here)

        return
        
    def usage(self, msg):
        print msg
        print 'Usage:'
        print '   ducc.py -c <process> [-n <numagents>] [-b] [-d date] [-o rmmem] [arguments ...]'
        print '   ducc.py -k'
        print 'Where:'
        print '   -c <component> is the name of the comp[onent to start, currently one of'
        print '                agent rm sm pm ws orchestrator broker'
        print '                      -- or --'
        print '                all - to start all but the agents'
        print '                head - to start rm sm pm ws orchestrator'
        print '        NOTE -- that agents should be started separately'
        print '   -n <numagents> if > 1, multiple agents are started (testing mode)'
        print '   -b uses nohup and places the process into the background'
        print '   -d date is the data on the caller, for startup verification'
        print '   -o <mem-in-GB> rm memory override for use on small machines'
        print '   -k causes the entire DUCC system to shutdown'
        print '   --nodup If specified, do not start a process if it appears to be already started.'
        print '   --or_parms [cold|warm]'
        print '   --simtest If specified, use unblocked broker for sim tests.'
        print '   arguments - any additional arguments to pass to the component.'
        sys.exit(1)
    
    def main(self, argv):
        
        component = None
        numagents = 1
        rmoverride = '0'
        args = None
        shutdown = False
        background = False
        or_parms = None
        nodup = False           # we allow duplicates unless asked not to
        localdate = time.time()
        simtest = False

        try:
           opts, args = getopt.getopt(argv, 'bc:d:n:o:sk?v', ['or_parms=', 'nodup', 'simtest' ])
        except:
            self.usage('Bad arguments ' + ' '.join(argv))
    
        for ( o, a ) in opts:
            if ( o == '-c' ) :
                component = a
                if ( component == 'or' ):
                    component = 'orchestrator'                
            elif ( o == '-b'):
                background = True
            elif ( o == '-d'):
                localdate = float(a)
            elif ( o == '-n'):
                numagents = int(a)
            elif ( o == '-o'):
                rmoverride = a
            elif ( o == '-k'):
                shutdown = True
            elif ( o == '--or_parms' ):
                or_parms = a
            elif ( o == '--nodup' ):
                nodup = True
            elif ( o == '--simtest' ):
                simtest = True
            elif ( o == '-v'):
                self.version()
            else:
                print 'badarg', a
                usage('bad arg: ' + a)

        if ( shutdown ):
            if ( component != None ):
                print 'Note: -c flag for component not allowed when shutting down. Shutdown aborted'
                sys.exit(1);
            self.clean_shutdown();
            sys.exit(1)

        if ( component == None ):
            self.usage("Must specify component")

        if ( component == 'db' ):
            self.run_db()
            return

        if ( component == 'broker' ):
            self.run_broker(simtest)
            return

        # fall-through, runs one of the ducc components proper
        self.run_component(component, or_parms, numagents, rmoverride, background, nodup, localdate)

        return

    def __call__(self, *args):
        self.main(args)
        return
        
if __name__ == "__main__":
    ducc = Ducc()
    ducc.main(sys.argv[1:])
    
