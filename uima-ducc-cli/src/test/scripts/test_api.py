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
import subprocess

# 
# Run the full set of functional tests
#

#
# Many things are time sensitive as DUCC failures can cause some of the calls to never return.
# Your ducc.properties should have the following (or even smaller) tuning values set.
#
# ducc.sm.instance.failure.max                          3
# ducc.sm.instance.failure.window                      10
# ducc.sm.meta.ping.rate                            10000
# ducc.sm.default.linger                            15000
# ducc.orchestrator.state.publish.rate              10000
# ducc.rm.state.publish.ratio                           1
# ducc.agent.node.metrics.publish.rate              10000
# ducc.agent.node.inventory.publish.rate            10000
# ducc.pm.state.publish.rate                        15000
# ducc.agent.launcher.process.stop.timeout           5000
#
#
# map the test id to the proper test class
#
testset = { 
    '1' : 'TestCommandLine',
    '2' : 'ClassSeparation',
    '3' : 'ReserveAndCancel',
    '4' : 'SubmitAndCancel',
    '5' : 'ManagedReserveAndCancel',
    '6' : 'ServiceTester',
    }

keys = testset.keys()

def usage(*msg):
    if ( msg[0] != None ):
        print ' '
        print 'Error:', ' '.join(msg)
        print ' '

    print 'Usage:'
    print '    test_api.py --ducc_home <path-to-ducc-home> <testid>'
    print ' '
    print 'Where:'
    print '    <path-to-ducc-home> is the full path to th einstalled Ducc Runtime to run against.'          
    print ' '
    print '    <testid> any of the following.  Multiple ids may be specified in a single run. If you'
    print '    specify the same id multple times it will be run multiple times.'
    print '        all: Run all tests'

    for k in keys:
        print '        ' + k + ":", testset[k]

    sys.exit(1)

def main():

    test_classes = '../../../target/test-classes'
    DUCC_HOME = None

    argv = sys.argv[1:]

    #
    # get DUCC_HOME from caller
    #
    try:
        opts, args = getopt.getopt(argv, '-h?', ['ducc_home='])
    except:
        usage("Invalid arguments " + ' '.join(argv))

    for ( o, a ) in opts:
        if o in ('-h', '-?'):
            usage(None)
        elif o in ('--ducc_home'):
            DUCC_HOME = a
        else:
            usage('Invalid argument ' + a)

    if ( DUCC_HOME == None ):
        usage("Missing ducc_home")

    #
    # swap in all keys for 'all' and check for validity of specified test ids
    #
    realargs = []
    for a in args:
        if ( a == 'all' ):
            realargs = realargs + keys
        elif (a in keys):
            realargs.append(a)
        elif ( a in '?' ):
            usage(None)
        else:
            usage('Invalid testid:', a)

    #
    # find the configured java
    #
    sys.path.append(DUCC_HOME + '/bin')
    from ducc_base  import DuccProperties
    props = DuccProperties()
    props.load(DUCC_HOME + '/resources/ducc.properties')
    java = props.get('ducc.jvm')

    #
    # establish test classpath
    #
    parts = [test_classes,                                      # test code
             DUCC_HOME + '/lib/uima-ducc-cli.jar',              # DUCC API
             DUCC_HOME + '/apache-uima/lib/uima-core.jar',      # Uima, for service tests
             DUCC_HOME + '/apache-uima/lib/uimaj-as-core.jar',  # Uima-as, for service tests
             ]
    CLASSPATH = ':'.join(parts)

    #
    # run the tests
    #
    failures = []
    for k in realargs:
        print '============================== Test', k, ':', testset[k], '=============================='
        CMD = [java, '-DDUCC_HOME='+DUCC_HOME, '-cp', CLASSPATH, 'org.apache.uima.ducc.cli.test.' + testset[k]]
        CMD = ' '.join(CMD)
        print CMD
        proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        for lines in proc.stdout:
            if ( 'FAILURE' in lines ) :
                failures.append((testset[k], lines))
            print lines.strip()

    os.system('rm -rf *.output')
    # 
    # finally summarize failures
    #
    if (len(failures) > 0 ):
        print '============================== Failed tests =============================='
        for f in failures:
            print f[0], f[1]
        
main()
