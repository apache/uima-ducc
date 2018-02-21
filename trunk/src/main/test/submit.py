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
    print "Usage: submit.py -d <DUCC_HOMe>"
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
        'driver_descriptor_CR'          : 'org.apache.uima.ducc.test.randomsleep.FixedSleepCR',
        'driver_descriptor_CR_overrides': 'jobfile=' + ducc_home + '/examples/simple/1.inputs compression=10 error_rate=0.0',
        'driver_jvm_args'               : '-Xmx500M',
        
        'process_descriptor_AE'         : 'org.apache.uima.ducc.test.randomsleep.FixedSleepAE',
        'process_memory_size'           : '2',
        'classpath'                     : ducc_home + '/lib/uima-ducc/examples/*',
        'process_jvm_args'              : '-Xmx100M ',
        'process_thread_count'          : '2',
        'process_per_item_time_max'     : '5',
        'environment'                   : 'AE_INIT_TIME=5 AE_INIT_RANGE=5 INIT_ERROR=0 LD_LIBRARY_PATH=/yet/a/nother/dumb/path',
        'process_deployments_max'       : '999',
        
        'scheduling_class'              : 'normal',  
        'working_directory'             : os.environ['HOME'],
        }

    testno = testno + 1
    print '------------------------------ Test', testno, 'Submit ------------------------------'
    props['description'] = 'Submit test ' + str(testno)
    CMD = mkcmd('ducc_submit', props)
    os.system(CMD)


    testno = testno + 1
    print '------------------------------ Test', testno, 'Submit and Cancel ------------------------------'
    props['description'] = 'Submit test ' + str(testno)
    CMD = mkcmd('ducc_submit', props)
    os.system(CMD)
    proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    stdout = proc.stdout
    for line in stdout:
        line = line.strip()
        toks = line.split()

    print ' ... pause 10 seconds and then cancel ...'
    time.sleep(10)

    CMD = mkcmd('ducc_cancel', {'id': toks[1]})
    os.system(CMD)

    testno = testno + 1
    print '------------------------------ Test', testno, 'Submit And Wait For Completion ------------------------------'
    props['description'] = 'Submit test ' + str(testno)
    props['wait_for_completion'] = None 
    CMD = mkcmd('ducc_submit', props)
    os.system(CMD)

    testno = testno + 1
    print '------------------------------ Test', testno, 'Submit With Attached Console ------------------------------'
    del props['wait_for_completion']
    props['description'] = 'Submit test ' + str(testno)
    props['attach_console'] = None 
    CMD = mkcmd('ducc_submit', props)
    os.system(CMD)

    testno = testno + 1
    print '------------------------------ Test', testno, 'Submit All In One Local ------------------------------'
    del props['attach_console']
    props['description'] = 'Submit test ' + str(testno)
    props['all_in_one'] = 'local'
    CMD = mkcmd('ducc_submit', props)
    os.system(CMD)

    testno = testno + 1
    print '------------------------------ Test', testno, 'Submit All In One Remote ------------------------------'
    props['description'] = 'Submit test ' + str(testno)
    props['all_in_one'] = 'remote'
    props['scheduling_class'] = 'fixed'
    CMD = mkcmd('ducc_submit', props)
    os.system(CMD)

    testno = testno + 1
    print '------------------------------ Test', testno, 'Submit All In One Bogus ------------------------------'
    props['description'] = 'Submit test ' + str(testno)
    props['all_in_one'] = 'bogus'
    CMD = mkcmd('ducc_submit', props)
    os.system(CMD)

    print '------------------------------ Submit Testing Complete  ------------------------------'

main(sys.argv[1:])
