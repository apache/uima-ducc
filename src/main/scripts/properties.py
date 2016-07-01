#!/usr/bin/python

import os
import re
import platform
import string

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

class PropertiesException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)

class Property:
    def __init__(self, k, v, c):
        self.k = k         # key
        self.v = v         # value
        self.c = c         # comments
        self.orig_v = v
    
    def reset(self):
        self.v = self.orig_v

    def __str__(self):
        return str(self.k) + '=' + str(self.v)

class Properties:
    def __init__(self):
        self.props = {}
        self.builtin = {}

        self.keys = []
        self.comments = []

        #
        # Create builtins corresponding to some of the java properties.
        #
        # We allow expansion on java system properties.  It's obviously not possible to
        # do most of them but these guys may have a use e.g. to put ducc_ling into
        # architecture-specific places.
        #
        (system, node, release, version, machine, processor) = platform.uname()
        if ( system == 'Darwin' ):
            self.builtin['os.arch'] = 'x86_64'
            self.builtin['os.name'] = 'Mac OS X'
        elif ( system == 'Linux' ):
            if ( machine == 'ppc64' ):
                self.builtin['os.arch'] = 'ppc64'
                self.builtin['os.name'] = 'Linux'
            elif ( machine == 'x86_64' ):
                self.builtin['os.arch'] = 'amd64'
                self.builtin['os.name'] = 'Linux'
            elif ( machine == 'ppc64le' ):
                self.builtin['os.arch'] = 'ppc64le'
                self.builtin['os.name'] = 'Linux'


    #
    # Expand all ${} values. The search order is:
    #    1 look in this properties file
    #    2 look in the environment
    #    3 look in a subset of the Java system properties (os.name & os.arch)
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
            elif ( os.environ.has_key(key) ):
                val = os.environ[key]                
            elif (self.builtin.has_key(key) ):
                val = self.builtin[key]

            if ( val != None ):    
                response = string.replace(response, m.group() , val)
            ndx = m.start()+1
            m = p.search(response, ndx)
        
        return response

    def mkitem(self, line):
        #
        # First deal with line comments so we can preserve them on write
        #
        if ( line.startswith('#') ):
            self.comments.append(line)
            return False

        if ( line.startswith('//') ):
            self.comments.append(line)
            return False

        if ( line == '' ):
            return False

        #
        # Now strip off embedded comments, these are lost, but they're not valid
        # for java props anyway.
        #
        ndx = line.find('#')   # remove comments - like the java DuccProperties
        if ( ndx >= 0 ):
            line = line[0:ndx]     # strip the comment
        ndx = line.find('//')   # remove comments - like the java DuccProperties
        if ( ndx >= 0 ):
            line = line[0:ndx]     # strip the comment
        line = line.strip()    # clear leading and trailing whitespace
        
        if ( line == '' ):
            return 

        mobj = re.search('[ =:]+', line)
        if ( mobj ):            
            key = line[:mobj.start()].strip()
            val = line[mobj.end():].strip()
            # print 'NEXT', mobj.start(), 'END', mobj.end(), 'KEY', key, 'VAL', val
            # val = self.do_subst(val)   # we'll do lazy subst on get instead
            self.props[key] = Property(key, val, self.comments)
            if ( key in self.keys ):
                self.keys.remove(key)
            self.keys.append(key)
            self.comments = []
        else:
            self.props[line] = Property(line, '', self.comments)
            self.keys.append(line)
            self.comments = []

    #
    # Load reads a properties file and adds it contents to the
    # hash.  It may be called several times; each call updates
    # the internal has, thus building it up.  The input file is
    # in the form of a java-like properties file.
    #
    def load(self, propsfile):
        if ( not os.path.exists(propsfile) ):
            raise PropertiesException(propsfile +  ' does not exist and cannot be loaded.')

        f = open(propsfile);
        for line in f:
            self.mkitem(line.strip())
        f.close()

    # read a jar manifest into a properties entity
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
    # Put something into the hash with an optional comment
    #
    def put(self, key, value, comment=[]):
        self.props[key] = Property(key, value, comment)
        self.keys.append(key)

    #
    # Put a Property object into the map
    #
    def put_property(self, p):
        self.props[p.k] = p
        self.keys.append(p.k)

    #
    # Get a value from the hash
    #
    def get(self, key):
        if ( self.props.has_key(key) ):
            return self.do_subst(self.props[key].v)   # we'll do lazy subst on get instead
        return None

    #
    # Get a Property object for manipulation (k, v, comment)
    #
    def get_property(self, key):
        if ( self.props.has_key(key) ):
            return self.props[key]                    # note no expansion.  
        return None

    #
    # Remove an item if it exists
    #
    def delete(self, key):
        if ( self.props.has_key(key) ):
            del self.props[key]
            self.keys.remove(key)

    #
    # Write the has as a Java-like properties file
    #
    def write(self, propsfile):
        f = open(propsfile, 'w')
        for k in self.keys:
            p = self.props[k]
            v = p.v
            c = p.c
            for cc in c:
                f.write(cc + '\n')
            f.write(k + ' = ' + str(v) + '\n\n')

        f.close()

    #
    # return a shallow copy of the dictionary
    #
    def copy_dictionary(self):
        return self.props.copy()

    #
    # Return the entries (Property list) in the dictionary
    #
    def items(self):
        return self.props.items()

    #
    # The keys, in the order as defined in the input file
    #
    def get_keys(self):
        return self.keys

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
