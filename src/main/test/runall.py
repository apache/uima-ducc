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

def usage(msg):
    if ( msg != None ):
        print msg
    print "Usage: runall.py -d <DUCC_HOMe>"
    print "   DUCC_HOME is the full path to your installed, working, and running ducc installation."
    print ''
    print 'This runs all the tests'
    sys.exit(1);

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
    os.system("./reserve.py -d " + ducc_home)
    os.system("./managed_reserve.py -d " + ducc_home)
    os.system("./service.py -d " + ducc_home)
    os.system("./submit.py -d " + ducc_home)

main(sys.argv[1:])
