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

import re
import os
import sys
import time
import getopt
import subprocess

import string
# Must be self-contained using only the DUCC_HOME runtime
#
# Python-based CLI equivalent of the API tests
#

class DuccPropertiesException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class DuccProperties:
    # copied in from ducc_base to read ducc.properties.  maybe should make into its own file some day
    def __init__(self):
        self.props = {}
        self.builtin = {}

    #
    # Expand all ${} values from env or from this properties file itself
    # The search order is:
    #    1 look in this properties file
    #    2 look in the environment
    #
    def do_subst(self, st):
        key = None
        p = re.compile("\\$\\{[a-zA-Z0-9_\\.\\-]+\\}")
        ndx = 0
        response = st.strip()
        m = p.search(response, ndx)    
        while ( m != None ):
            key = m.group()[2:-1]
            
            val = None
            if ( self.has_key(key) ):
                val = self.get(key)
            elif (self.builtin.has_key(key) ):
                val = self.builtin[key]

            if ( val != None ):    
                response = string.replace(response, m.group() , val)
            ndx = m.start()+1
            m = p.search(response, ndx)
        
        return response

    def mkitem(self, line):
        ndx = line.find('#')   # remove comments - like the java DuccProperties
        if ( ndx >= 0 ):
            line = line[0:ndx]     # strip the comment
        ndx = line.find('//')   # remove comments - like the java DuccProperties
        if ( ndx >= 0 ):
            line = line[0:ndx]     # strip the comment
        line = line.strip()    # clear leading and trailing whitespace
        if ( line == '' ):     # empty line?
            return

        mobj = re.search('[ =:]+', line)
        if ( mobj ):
            key = line[:mobj.start()].strip()
            val = line[mobj.end():].strip()
            #print 'NEXT', mobj.start(), 'END', mobj.end(), 'KEY', key, 'VAL', val
            # val = self.do_subst(val)   # we'll do lazy subst on get instead
            self.props[key] = val
        else:
            self.props[line] = None

    #
    # Load reads a properties file and adds it contents to the
    # hash.  It may be called several times; each call updates
    # the internal has, thus building it up.  The input file is
    # in the form of a java-like properties file.
    #
    def load(self, propsfile, ducchome=None):
        if ( not os.path.exists(propsfile) ):
            raise DuccPropertiesException(propsfile +  ' does not exist and cannot be loaded.')

        if (ducchome != None):
            self.props['DUCC_HOME'] = ducchome
        f = open(propsfile);
        for line in f:
            self.mkitem(line.strip())
        f.close()

    def load_from_manifest(self, jarfile):
        z = zipfile.ZipFile(jarfile)
        items = z.read('META-INF/MANIFEST.MF').split('\n')
        for item in items:
            self.mkitem(item)

    #
    # Try to load a properties file.  Just be silent if it doesn't exist.
    #
    def load_if_exists(self, propsfile):
        if ( os.path.exists(propsfile) ):
            return self.load(propsfile)
        
    #
    # Put something into the hash.
    #
    def put(self, key, value):
        self.props[key] = value

    #
    # Get something from the hash.
    #
    def get(self, key):
        if ( self.props.has_key(key) ):
            return self.do_subst(self.props[key])   # we'll do lazy subst on get instead
        return None

    #
    # Remove an item if it exists
    #
    def delete(self, key):
        if ( self.props.has_key(key) ):
            del self.props[key]
    #
    # Write the has as a Java-like properties file
    #
    def write(self, propsfile):
        f = open(propsfile, 'w')
        items = self.props.items()
        for (k, v) in items:
            #print 'WRITING', k, '=', v
            f.write(k + ' = ' + str(v) + '\n')
        f.close()

    #
    # return a shallow copy of the dictionary
    #
    def copy_dictionary(self):
        return self.props.copy()

    #
    # return the entries in the dictionary
    #
    def items(self):
        return self.props.items()

    #
    # check to see if the key exists in the dictionary
    #
    def has_key(self, key):
        return self.props.has_key(key)

    #
    # Return the length of the dictionary
    #
    def __len__(self):
        return len(self.props)

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
    return CMD

def mkBrokerUrl(ducc_home):
    props = DuccProperties()
    props.load(ducc_home + '/resources/ducc.properties')
    protocol = props.get('ducc.broker.protocol')
    host     = props.get('ducc.broker.hostname')
    port     = props.get('ducc.broker.port')
    return protocol + '://' + host + ':' + port


def waitForState(state, sid):
    print 'Waiting for state', state
    while True:
        CMD = mkcmd('ducc_services', {'query':sid})
        proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        stdout = proc.stdout
        for line in stdout:
            line = line.strip()
            if ( 'Service State' in line ):
                if ( state in line ):
                    return
                else:
                    print ' ...', line
        time.sleep(5)


def waitForEnable(sid):
    print 'Waiting for service to be Enabled'
    while True:
        CMD = mkcmd('ducc_services', {'query':sid})
        proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        stdout = proc.stdout
        for line in stdout:
            line = line.strip()
            if ( 'Start Mode' in line ):
                if ( 'Enabled' in line ):
                    return
                else:
                    print line
        time.sleep(5)

def waitForDisable(sid):
    print 'Waiting for service to be Disabled'
    while True:
        CMD = mkcmd('ducc_services', {'query':sid})
        proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        stdout = proc.stdout
        for line in stdout:
            line = line.strip()
            if ( 'Start Mode' in line ):
                if ( 'Disabled' in line ):
                    return
                else:
                    print line
        time.sleep(5)

def waitForStartState(state, sid):
    print 'Waiting for service start state', state
    while True:
        CMD = mkcmd('ducc_services', {'query':sid})
        proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        stdout = proc.stdout
        for line in stdout:
            line = line.strip()
            if ( 'Start Mode' in line ):
                if ( state in line ):
                    return
                else:
                    print ' ...', line
        time.sleep(5)


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

    broker = mkBrokerUrl(ducc_home)
    print broker

    props = {
        "process_jvm_args"      : "-Xmx100M -DdefaultBrokerURL=" + broker,
        "classpath"             : "${DUCC_HOME}/lib/uima-ducc/examples/*:${DUCC_HOME}/apache-uima/lib/*:${DUCC_HOME}/apache-uima/apache-activemq/lib/*:${DUCC_HOME}/examples/simple/resources/service",
        "service_ping_arguments": "broker-jmx-port=1099",
        "environment"           : "AE_INIT_TIME=5000 AE_INIT_RANGE=1000 INIT_ERROR=0 LD_LIBRARY_PATH=/yet/a/nother/dumb/path",
        "process_memory_size"   : "15",
        "process_DD"            : "${DUCC_HOME}/examples/simple/resources/service/Service_FixedSleep_1.xml",
        "scheduling_class"      : "fixed",
        "working_directory"     : "${HOME}",
        "service_linger"        : "30000",
    }

    testno = testno + 1
    print '------------------------------ Test', testno, 'Clear services at start ------------------------------'
    CMD = mkcmd('ducc_services', {'query':None})
    proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    stdout = proc.stdout
    for line in stdout:
        line = line.strip()
        print line
        if ( 'Service Class' in line ):
            toks = line.split()
            sid = toks[6]
            user = re.split('[\[\]]', toks[7] )[1]
            if ( user == os.environ['LOGNAME'] ):
                print 'Unregister service', user, sid, line
                os.system(mkcmd('ducc_services', {'unregister':sid}))

    testno = testno + 1
    print '------------------------------ Test', testno, 'Register ------------------------------'
    props['description'] = 'Service test ' + str(testno)
    props['register'] = None
    CMD = mkcmd('ducc_services', props)
    proc = subprocess.Popen(CMD, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    stdout = proc.stdout
    for line in stdout:
        line = line.strip()
        print line
        if ( 'succeeded' in line ):
            toks = line.split()
            sid = re.split('[\[\]]', toks[7] )[1]
    print '======= service id', sid

    time.sleep(3)
    os.system(mkcmd('ducc_services', {'query':None}))

    testno = testno + 1
    print '------------------------------ Test', testno, 'Start ------------------------------'
    os.system(mkcmd('ducc_services', {'start':sid}))
    waitForState("Available", sid)
    print "Service is started:"
    os.system(mkcmd('ducc_services', {'query':sid}))

    testno = testno + 1
    print '------------------------------ Test', testno, 'Stop ------------------------------'
    os.system(mkcmd('ducc_services', {'stop':sid}))
    waitForState("Stopped", sid)
    print "Service is stopped:"
    os.system(mkcmd('ducc_services', {'query':sid}))

    testno = testno + 1
    print '------------------------------ Test', testno, 'Enable ------------------------------'
    os.system(mkcmd('ducc_services', {'enable':sid}))
    waitForEnable(sid)
    os.system(mkcmd('ducc_services', {'query':sid}))

    testno = testno + 1
    print '------------------------------ Test', testno, 'Disable ------------------------------'
    os.system(mkcmd('ducc_services', {'disable':sid}))
    waitForDisable(sid)
    os.system(mkcmd('ducc_services', {'query':sid}))

    testno = testno + 1
    print '------------------------------ Test', testno, 'Modify ------------------------------'
    os.system(mkcmd('ducc_services', {'enable':sid}))      # first enable it
    waitForEnable(sid)
    os.system(mkcmd('ducc_services', {'query':sid}))
    os.system(mkcmd('ducc_services', {'modify':sid, 'autostart':'true'}))

    print 'Waiting for service to start'
    waitForState("Available", sid)
    os.system(mkcmd('ducc_services', {'query':sid}))

    testno = testno + 1
    print '------------------------------ Test', testno, 'Switch to reference start 1 ------------------------------'
    os.system(mkcmd('ducc_services', {'modify':sid, 'autostart':'false'}))
    waitForStartState('manual', sid)      # this can take a few moments
    os.system(mkcmd('ducc_services', {'observe_references':sid}))      # first enable it
    waitForStartState('reference', sid)
    print 'Waiting for service to linger stop'
    waitForState("Stopped", sid)
    os.system(mkcmd('ducc_services', {'query':sid}))

    testno = testno + 1
    print '------------------------------ Test', testno, 'Switch to reference start 2 ------------------------------'
    print 'Start service in manual mode then switch to reference'
    os.system(mkcmd('ducc_services', {'start':sid} ))
    waitForState("Available", sid)
    os.system(mkcmd('ducc_services', {'query':sid}))
    os.system(mkcmd('ducc_services', {'observe_references':sid}))      # first enable it
    waitForStartState('reference', sid)
    os.system(mkcmd('ducc_services', {'query':sid}))

    print 'Waiting for service to linger stop'
    waitForState("Stopped", sid)
    os.system(mkcmd('ducc_services', {'query':sid}))


    testno = testno + 1
    print '------------------------------ Test', testno, 'Switch to reference start 3 ------------------------------'
    print 'Start service in manual mode then switch to reference, then back before it stops'
    os.system(mkcmd('ducc_services', {'start':sid} ))                # start in manual
    waitForState("Available", sid)
    os.system(mkcmd('ducc_services', {'query':sid}))

    os.system(mkcmd('ducc_services', {'observe_references':sid}))    # swith to reference
    waitForStartState('reference', sid)
    os.system(mkcmd('ducc_services', {'query':sid}))

    os.system(mkcmd('ducc_services', {'ignore_references':sid}))      # switch back to manual
    waitForStartState('manual', sid)
    os.system(mkcmd('ducc_services', {'query':sid}))

    os.system(mkcmd('ducc_services', {'stop':sid} ))                 # done, stop
    print 'Waiting for service to linger stop'
    waitForState("Stopped", sid)
    os.system(mkcmd('ducc_services', {'query':sid}))

    testno = testno + 1
    print '------------------------------ Test', testno, 'Unregister ------------------------------'
    print 'Unregisteres the service before completion of the test.'
    os.system(mkcmd('ducc_services', {'unregister':sid} ))                # start in manual

    print '------------------------------ Service Testing Complete  ------------------------------'

main(sys.argv[1:])
