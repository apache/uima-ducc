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

from ducc_util import DuccUtil
from ducc_util import DuccProperties

class Ducc(DuccUtil):
        
    def run_component(self, component, or_parms, numagents, rmoverride, background, nodup):

        if ( component == 'all' ):
            component = 'rm,sm,pm,ws,orchestrator'
    
        complist = component.split(',')
        args = None

        jvm_opts = []
        jvm_opts.append('-Dos.page.size=' + self.os_pagesize)
        jvm_opts.append('-Dducc.deploy.configuration=' + self.DUCC_HOME + '/resources/ducc.properties')
 
        service = 'org.apache.uima.ducc.common.main.DuccService'
        for c in complist:
            if ( c == 'agent' ):
                if ( len(complist) > 1 ):
                    print "Must start agents separately"
                    sys.exit(1)
                    
                if ( not self.verify_duccling() ):
                    print 'ducc_ling is not set up correctly on node', self.localhost
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

            if ( c == 'viz' ):
                here = os.getcwd()
                os.chdir(self.DUCC_HOME + '/vizserver')
                if ( self.ws_jvm_args != None ):
                    jvm_opts.append(self.ws_jvm_args)

            if ( c == 'orchestrator' ):
                if ( or_parms != '' ):
                    args = '-' + or_parms
                if ( self.or_jvm_args != None ):
                    jvm_opts.append(self.or_jvm_args)

            if ( c == 'pm' ):
                if ( self.pm_jvm_args != None ):
                    jvm_opts.append(self.pm_jvm_args)

            if ( c == 'sm' ):
                if ( self.sm_jvm_args != None ):
                    jvm_opts.append(self.sm_jvm_args)

                                        
        if (component != 'agent'):
            service = 'org.apache.uima.ducc.common.main.DuccService'
            ducc_component = '-Dducc.deploy.components=' + component

        # check to see if there is a process like this running already, and barf if so
        pid = None
        if ( nodup ):
            response = self.find_ducc_process(self.localhost, os.environ['LOGNAME'])   # always make this "me"
            if ( len(response) > 0 ):
                for p in response:
                    if ( p[0] == component ):
                        print "Component", component,'is already running in PID', p[1], 'on node', self.localhost
                        pid = p[1]

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
                print 'PID ' + pid      # nohup will print this from the (twice) forked process if background
                                        # hard for us to access it here in nohup

        if ( (c == 'ws') or ( c == 'viz') ):
            os.chdir(here)

        return
        
    def usage(self, msg):
        print msg
        print 'Usage:'
        print '   ducc.py -c <process> [-n <numagents>] [-b] [arguments ...]'
        print '   ducc.py -k'
        print 'Where:'
        print '   -c <component> is the name of the comp[onent to start, currently one of'
        print '                agent rm sm pm ws orchestrator'
        print '                      -- or --'
        print '                all - to start rm sm pm ws orchestrator'
        print '        NOTE -- that agents should be started separately'
        print '   -b uses nohup and places the process into the background'
        print '   -n <numagents> if > 1, multiple agents are started (testing mode)'
        print '   -o <mem-in-GB> rm memory override for use on small machines'
        print '   -k causes the entire DUCC system to shutdown'
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

        try:
           opts, args = getopt.getopt(argv, 'bc:n:o:k?v', ['or_parms=', 'nodup'])
        except:
            self.usage('Bad arguments ' + ' '.join(argv))
    
        for ( o, a ) in opts:
            if ( o == '-c' ) :
                component = a
                if ( component == 'or' ):
                    component = 'orchestrator'
                
            elif ( o == '-b'):
                background = True
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

        self.run_component(component, or_parms, numagents, rmoverride, background, nodup)
        return

    def __call__(self, *args):
        self.main(args)
        return
        
if __name__ == "__main__":
    ducc = Ducc()
    ducc.main(sys.argv[1:])
    
