#!/usr/bin/env python

import os

# common routines for ducc_post_install and db_create
def addToCp(cp, lib):
    return cp + ':' + lib


def execute(CMD):
    print CMD
    return os.system(CMD)

def stop_database(pidfile):
    print "Stopping the dtabase."

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
        


def configure_database(DUCC_HOME, DUCC_HEAD, java, db_pw):
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

    if ( os.path.exists(DUCC_HOME + "/database/data") ):
        print 'Database is already defined in', DUCC_HOME + '/database', '- not rebilding.'
        return False


    if ( db_pw == 'bypass' ):
        print 'Database support will be bypassed'
        return True
        
    update_cassandra_config(DUCC_HOME, DUCC_HEAD)

    here = os.getcwd()
    os.chdir(DUCC_HOME + "/cassandra-server")
    pidfile = DUCC_HOME + '/state/cassandra.pid'
    print 'Starting the database.  This might take a few moments if it is the first time.'
    CMD = "bin/cassandra -p "+  pidfile + " > /dev/null 2>&1";
    os.system(CMD);
    print "Database is started.  Waiting for initialization";
    os.chdir(here) 

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
    CMD = [java, '-DDUCC_HOME=' + DUCC_HOME, 'org.apache.uima.ducc.database.DbCreate', DUCC_HEAD, 'ducc', db_pw]
    CMD = ' '.join(CMD)
    if ( execute(CMD) == 0 ):
        print 'Database is initialized.'
    else:
        print 'Database started but the schema could not be defined. DB logs are in', DUCC_HEAD + '/cassandra-server/logs.'
        ret = False

    stop_database(pidfile)
    return ret
