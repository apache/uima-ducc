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
import time
import getopt
import subprocess

# Must be self-contained using only the DUCC_HOME runtime
#
# Python-based CLI equivalent of the API tests
#

def usage(msg):
    if ( msg != None ):
        print msg
    print "Usage: managed_reserve.py -d <DUCC_HOMe>"
    print "   DUCC_HOME is the full path to your installed, working, and running ducc installation."
    sys.exit(1);

def mkcmd(CMD, props):
    CMD = os.environ['DUCC_HOME'] + '/bin/' + CMD
    for k in props.keys():
        v = props[k]
        if ( v == None ):
            CMD = CMD + ' --' + k 
        else:
            CMD = CMD + ' --' + k + ' ' + "'" + props[k] + "'"
    print CMD
    return CMD

def main(argv):

    ducc_home = None
    testno = 0

    try:
        opts, args = getopt.getopt(argv, 'd:', ['ducc='])
    except:
        usage("Invalid arguments " + ' '.join(argv))

    for ( o, a ) in opts:
        if o in ('-d', '--ducc'):
            ducc_home = a
        else:
            usage("Illegal arguments")


    if ( ducc_home == None ):
        usage('No ducc home')

    os.environ['DUCC_HOME'] = ducc_home
    print 'Starting test'
        
    props = {
        'process_memory_size' : "4",
        'process_executable'      : '/bin/sleep',
        'process_executable_args' : '5',
        'scheduling_class'        : 'fixed',  
        }

    testno = testno + 1
    print '------------------------------ Test', testno, 'Managed Reserve ------------------------------'
    props['description'] = 'Managed Reservation test ' + str(testno)
    CMD = mkcmd('ducc_process_submit', props)
    os.system(CMD)

    testno = testno + 1
    print '------------------------------ Test', testno, 'Managed Reserve ------------------------------'
    props['description'] = 'Managed Reservation test ' + str(testno)
    props['process_executable_args'] = '300'
    CMD = mkcmd('ducc_process_submit', props)
    proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    stdout = proc.stdout
    for line in stdout:        
        line = line.strip()
        print line
        toks = line.split()
        resid = toks[2]

    print 'Waiting 20 seconds for reservation to start, then cancel'
    time.sleep(20) 
    CMD = mkcmd('ducc_process_cancel', {'id':resid} )
    os.system(CMD)

    testno = testno + 1
    print '------------------------------ Test', testno, 'Managed Reservation with attached console ------------------------------'
    props['description'] = 'Managed Reservation test ' + str(testno)
    props['process_executable'] = '/bin/ls'
    props['process_executable_args'] = '-atl ' + os.environ['HOME']
    props['attach_console'] = None
    CMD = mkcmd('ducc_process_submit', props)

    os.system(CMD)

    testno = testno + 1
    print '------------------------------ Test', testno, 'Managed Reservation with wait for completion ------------------------------'
    props['description'] = 'Managed Reservation test ' + str(testno)
    props['process_executable'] = '/bin/sleep'
    props['process_executable_args'] = '10'
    del props['attach_console']
    props['wait_for_completion'] = None
    CMD = mkcmd('ducc_process_submit', props)

    os.system(CMD)

    print '------------------------------ Managed Reserve Testing Complete  ------------------------------'

main(sys.argv[1:])
