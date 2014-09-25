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
    print "Usage: reserve.py -d <DUCC_HOMe>"
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
        'instance_memory_size' : "4",
        'number_of_instances'  : "1",
        'scheduling_class'              : 'fixed',  
        }

    testno = testno + 1
    print '------------------------------ Test', testno, 'Reserve and Cancel ------------------------------'
    props['description'] = 'Reserve test ' + str(testno)
    CMD = mkcmd('ducc_reserve', props)
    proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    stdout = proc.stdout
    for line in stdout:        
        line = line.strip()
        print line
        if ( 'submitted' in line ):
            toks = line.split()
            resid = toks[1]
    
    CMD = mkcmd('ducc_unreserve', {'id': resid} )
    os.system(CMD)

    print '------------------------------ Reserve Testing Complete  ------------------------------'

main(sys.argv[1:])
