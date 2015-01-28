#!/usr/bin/python

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
import string
import subprocess
import re
import zipfile
import platform

from properties import *

def find_ducc_home():
    # Infer DUCC_HOME from our location - no longer use a (possibly inaccurate) environment variable
    me = os.path.abspath(__file__)    
    ndx = me.rindex('/')
    ndx = me.rindex('/', 0, ndx)
    DUCC_HOME = me[:ndx]          # split from 0 to ndx
    return DUCC_HOME

def find_localhost():
    return os.uname()[1]                

def which(file):
    for p in os.environ["PATH"].split(":"):
        if os.path.exists(p + "/" + file):
            return p + "/" + file            
    return None

    
class DuccBase:

    def read_properties(self):

        if ( self.do_merge ):
            self.merge_properties()

        self.ducc_properties = Properties()
        self.ducc_properties.put('ducc.home', self.DUCC_HOME)
        self.ducc_properties.put('DUCC_HOME', self.DUCC_HOME)
        self.ducc_properties.load(self.propsfile)

        self.webserver_node = self.ducc_properties.get('ducc.ws.node')
        self.jvm            = self.ducc_properties.get('ducc.jvm')

    def java(self):
        return self.jvm
        
    def java_home(self):
        if ( os.environ.has_key('DUCC_POST_INSTALL') ):
            return 'JAVA_HOME'   # avoid npe during first-time setup

        if ( self.system == 'Darwin' ):
            self.jvm_home = "/Library/Java/Home"
        else:
            ndx = self.jvm.rindex('/')
            ndx = self.jvm.rindex('/', 0, ndx)
            self.jvm_home = self.jvm[:ndx]

        return self.jvm_home
        
    def version(self):
        lines = self.popen(self.jvm, ' org.apache.uima.ducc.common.utils.Version')
        line = lines.readline().strip()
        return "DUCC Version", line
        
    # simply spawn-and-forget using Python preferred mechanism
    def spawn(self, *CMD):
        cmd = ' '.join(CMD)
        # print '**** spawn', cmd, '****'
        ducc = subprocess.Popen(cmd, shell=True)
        pid = ducc.pid
        try:
            status = os.waitpid(pid, 0)
        except KeyboardInterrupt:
            print 'KeyboardInterrupt'
        except:
            print "Unexpected exception: ", sys.exc_info()[0]
        return pid

    def popen(self, *CMD):
        cmd = ' '.join(CMD)
        #print 'POPEN:', cmd
        proc = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        return proc.stdout

    def format_classpath(self, cp):
        strings = cp.split(':')
        for s in strings:
            print s

    def set_classpath(self):
        ducc_home = self.DUCC_HOME
        LIB       = ducc_home + '/lib'
        
        CLASSPATH = LIB + '/ducc-submit.jar'
        os.environ['CLASSPATH'] = CLASSPATH

    def mkargs(self, args):
        '''
            The cli needs to insure all args are fully quoted so the shell doesn't
            lose the proper tokenization.  This quotes everything.
        '''
        answer = []
        for a in args:
            arg = '"' + a + '"'
            answer.append(arg)
        return answer

    def __init__(self, merge=False):
        self.DUCC_HOME = find_ducc_home()
        self.do_merge = merge
        self.ducc_properties = None

        self.system = platform.system()
        self.jvm = None
        self.webserver_node = 'localhost'
        self.propsfile = self.DUCC_HOME + '/resources/ducc.properties'
        self.localhost = find_localhost()

        self.read_properties()       

        os.environ['JAVA_HOME'] = self.java_home()

        self.set_classpath()

if __name__ == "__main__":
    base = DuccBase()

