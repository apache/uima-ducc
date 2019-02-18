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

#!/usr/bin/env python

import os

# common routines for ducc_post_install and db_create
def addToCp(cp, lib):
    return cp + ':' + lib


def execute(CMD):
    print CMD
    return os.system(CMD)

# --------------------------------------------------------------------------------
# these next methods are used to parse a table returned from cqlsh into a
#   - header
#   - dictionary of values for each row

# parse the header into a list of names
def parse_header(header):
    ret = []
    parts = header.split('|')
    for p in parts:
        ret.append(p.strip())
    return ret

# parse a single line into a dictionary with key from the header and value from the line
def parse_line(header, line):
    parts = line.split('|')
    ret = {}
    for k, v in zip(header, parts):
        ret[k] = v.strip()
    return ret

# parse a set of lines returned from cqlsh into a header and a list of dictionaries, one per line
# header_id is a sting we use to positively identify a header line
def parse(lines, header_id):
    ret = []
    header = []
    for l in lines:
        l = l.strip()
        # print '[]', l
        if ( l == '' ):
            continue
        if ( '---' in l ):
            continue;
        if ( 'rows)' in l ):
            continue;
        if ( header_id in l ):
            header = parse_header(l)
            continue
            
        ret.append(parse_line(header, l))

    return header, ret

# given a header and a collection of lines parsed by the utilities above, print a 
# mostly un-ugly listing of the table retults
def format(header, lines):
    
    # calculate max column widths
    hlens = {}
    for k in header:
        hlens[k] = len(k)
        for line in lines:
            if ( not hlens.has_key(k) ):
                hlens[k] = len(line[k])
            else:
                hlens[k] = max(len(line[k]), hlens[k])

    # create a format string from the widths 
    fmt = ''
    for k in header:
        fmt = fmt + ' %' + str(hlens[k]) + 's' 

    # first the header
    print fmt % tuple(header)

    # now the rows
    for line in lines:
        l = []
        for k in header:
            l.append(line[k])
        print fmt % tuple(l)
    return


# end of row parsing utilities
# --------------------------------------------------------------------------------

def stop_database(pidfile):
    print "Stopping the database."

    CMD = ['kill', '-TERM', '`cat ' + pidfile + '`']
    CMD = ' '.join(CMD)
    execute(CMD)

def manual_config(DUCC_HOME, DUCC_HEAD):
    print ''
    print 'To manually configure the database edit', DUCC_HOME + '/cassandra-server/conf/casssandra.yaml'
    print 'to Insure every occurance of DUCC_HEAD is replaced with', DUCC_HEAD, 'and every occurance'
    print 'of DUCC_HOME is replaced with', DUCC_HOME + '.'
    print ''
    print 'Note that one occurance of DUCC_HEAD will be quoted: you must preserve these quotes, e.g. as "' + DUCC_HEAD + '".'

def update_cassandra_config(DUCC_HOME, DUCC_HEAD):
    # Read cassandra.yaml and change the things necessary to configure it correctly
    config = DUCC_HOME + '/cassandra-server/conf/cassandra.yaml'
    f = open(config)
    lines = []
    for line in f:
        if ( line.startswith('listen_address:') ):
            line = line.strip();
            print 'Database host is configured at', line
            if ( not DUCC_HEAD in line ):
                print 'Must reconfigure listen_address to', DUCC_HEAD
                parts = line.strip().split(':')
                old = parts[1].strip()
                ch_head = "sed -i.bak s'/" + old + "/" + DUCC_HEAD + "'/ " + config
                os.system(ch_head)
        

def configure_database(DUCC_HOME, DUCC_HEAD, java, db_autostart=True, db_host=None, db_user=None, db_pw=None, db_replication=None):
    # for cassandra:
    # in ducc_runtime/cassandra-server/conf we need to update cassandra.yaml to establish
    # the data directories and db connection addresses

    # Note this is a bootstrap routine and doesn't try to use common code that may depend on
    # things being initialized correctly.

    if ( db_pw == None ):
        db_pw = raw_input("Enter database password OR 'bypass' to bypass database support:")
        if ( db_pw == '' ):
            print "Must enter a DB password or 'bypass' to continue."
        return False

    if ( db_pw == 'bypass' ):
        print 'Database support will be bypassed'
        return True

    if(db_host == None):
        db_host = DUCC_HEAD
    
    db_host = db_host.split()[0]
    print "database host: "+str(db_host)
    
    if( db_autostart ):
        if ( os.path.exists(DUCC_HOME + "/state/database/data") ):
            print 'Database is already defined in', DUCC_HOME + '/database', '- but will try to rebuild.'
        update_cassandra_config(DUCC_HOME, DUCC_HEAD)
        here = os.getcwd()
        os.chdir(DUCC_HOME + "/cassandra-server")
        pidfile = DUCC_HOME + '/state/cassandra.pid'
        consfile = DUCC_HOME + '/state/cassandra.configure.console'
        print 'Starting the database.  This might take a few moments if it is the first time.'
        CMD = "bin/cassandra -p "+  pidfile + " > "+consfile+" 2>&1";
        os.system(CMD);
        print "Database is started.  Waiting for initialization";
        os.chdir(here) 
    else:
        print "Database is not auto-managed.";
        
    # Now start the db and create the schema
    CLASSPATH = ''
    CLASSPATH = addToCp(CLASSPATH, DUCC_HOME + '/lib/cassandra/*')
    CLASSPATH = addToCp(CLASSPATH, DUCC_HOME + '/lib/guava/*')
    CLASSPATH = addToCp(CLASSPATH, DUCC_HOME + '/lib/apache-log4j/*')
    CLASSPATH = addToCp(CLASSPATH, DUCC_HOME + '/lib/uima-ducc/*')
    CLASSPATH = addToCp(CLASSPATH, DUCC_HOME + '/apache-uima/apache-activemq/lib/*')
    os.environ['CLASSPATH'] = CLASSPATH
    print os.environ['CLASSPATH']

    ret = True
    CMD = [java, '-DDUCC_HOME=' + DUCC_HOME, 'org.apache.uima.ducc.database.DbCreate', db_host, db_user, db_pw]
    if(db_replication != None):
        CMD.append(db_replication)
    CMD = ' '.join(CMD)
    if ( execute(CMD) == 0 ):
        print 'Database is initialized.'
    else:
        print 'Database schema could not be defined.'
        ret = False

    if( db_autostart ):
        stop_database(pidfile)
    return ret
