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

class DuccPropertiesException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)

class DuccProperties:
    def __init__(self):
        self.props = {}

    #
    # Expand all ${} values from env or from this properties file itself
    # The search order is:
    #    1 look in this properties file
    #    2 look in the environment
    #
    def do_subst(self, str):
        key = None
        p = re.compile("\\$\\{[a-zA-Z0-9_\\.\\-]+\\}")
        ndx = 0
        
        response = str
        m = p.search(response, ndx)    
        while ( m != None ):
            key = m.group()[2:-1]
            
            val = None
            if ( self.has_key(key) ):
                val = self.get(key)
            elif ( os.environ.has_key(key) ):
                val = os.environ[key]                

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
    def load(self, propsfile):
        if ( not os.path.exists(propsfile) ):
            raise DuccPropertiesException(propsfile +  ' does not exist and cannot be loaded.')

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
    
class DuccBase:


    def read_properties(self):

        self.ducc_properties = DuccProperties()
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
        lines = self.popen(self.jvm, ' org.apache.uima.ducc.utils.Version')
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

    def which(self, file):
        for p in os.environ["PATH"].split(":"):
            if os.path.exists(p + "/" + file):
                return p + "/" + file            
        return None

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

    def __init__(self):

        if ( os.environ.has_key('DUCC_HOME') ):
            self.DUCC_HOME = os.environ['DUCC_HOME']
        else:
            me = os.path.abspath(sys.argv[0])    
            ndx = me.rindex('/')
            ndx = me.rindex('/', 0, ndx)
            self.DUCC_HOME = me[:ndx]          # split from 0 to ndx
            os.environ['DUCC_HOME'] = self.DUCC_HOME
        

        self.system = platform.system()
        self.jvm = None
        self.webserver_node = 'localhost'
        self.propsfile = self.DUCC_HOME + '/resources/ducc.properties'
        self.localhost = os.uname()[1]                
        self.read_properties()       

        os.environ['JAVA_HOME'] = self.java_home()

        self.set_classpath()

if __name__ == "__main__":
    base = DuccBase()

