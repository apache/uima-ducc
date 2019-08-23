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
import grp
import pwd
import resource
import time
import platform
import httplib

from threading import *
import traceback
import Queue

from  stat import *
from local_hooks import find_other_processes

# Catch the annoying problem when the current directory has been changed, e.g. by installing a new release
try:
    os.getcwd()
except:
    print "ERROR getting current directory ... may have been replaced .. try cd'ing to it again"
    sys.exit(1)

# simple bootstrap to establish DUCC_HOME and to set the python path so it can
# find the common code in DUCC_HOME/admin
# Infer DUCC_HOME from our location - no longer use a (possibly inaccurate) environment variable
me = os.path.abspath(__file__)    
ndx = me.rindex('/')
ndx = me.rindex('/', 0, ndx)
DUCC_HOME = me[:ndx]          # split from 0 to ndx
    
sys.path.append(DUCC_HOME + '/bin')
from ducc_base import DuccBase
from properties import Properties

from ducc_logger import DuccLogger
logger = DuccLogger()   

import db_util as dbu

global use_threading
use_threading = True

ducc_util_debug_flag = False

def debug(label,data):
    if(ducc_util_debug_flag):
        print label, data

# The "ducc" userid is the user that installed DUCC and created this file.
# If the admin dir's permissions were 700 then could assume the current user is the ducc user
def find_ducc_uid():
    my_file = os.path.abspath(__file__)    
    my_stat = os.stat(my_file)
    my_uid = my_stat.st_uid
    pwdinfo = pwd.getpwuid(my_uid)
    return pwdinfo.pw_name

def base_dir():
        fpath = __file__.rsplit('/',2)[0]
        return fpath

class ThreadWorker(Thread):
    def __init__(self, queue, outlock):
        Thread.__init__(self)
        self.queue = queue
        self.outlock = outlock
        
    def run(self):
        while True:
            (method, args) = self.queue.get()

            if ( args == 'quit' ):
                self.queue.task_done()
                return;

            try:
                response = method(args)
                if ( response != None and len(response) > 0):
                    self.outlock.acquire()
                    for l in response:
                        print ' '.join(l)
                    self.outlock.release()
            except:
                print "Exception executing", str(method), str(args)
                traceback.print_exc()

            self.queue.task_done()

class ThreadPool:
    def __init__(self, size):
        if ( use_threading ):
            self.size = size
            self.queue = Queue.Queue()
            outlock = Lock()

            MAX_NPSIZE = 100
            if ( self.size > MAX_NPSIZE ):
                self.size = MAX_NPSIZE

            for i in range(self.size):
                worker = ThreadWorker(self.queue, outlock)
                worker.start()

    def invoke(self, method, *args):
        if ( use_threading ):
            self.queue.put((method, args))
        else:
            response = method(args)
            if ( response != None and len(response) > 0):
                for l in response:
                    print ' '.join(l)
            
    def quit(self):
        if ( use_threading ):
            for i in range(self.size):
                self.queue.put((None, 'quit'))

            print "Waiting for Completion"
            self.queue.join()
            print "All threads returned"
        else:
            print 'All Work completed'

class DuccUtil(DuccBase):

    def makedirs(self,dir):
        try:
            os.makedirs(dir)
            print 'make: '+dir
        except:
            pass
        
    def update_properties(self):

        if ( self.ducc_properties == None ):
            DuccBase.read_properties(self)

        self.ssh_enabled       = self.ducc_properties.get('ducc.ssh')
        self.duccling          = self.ducc_properties.get('ducc.agent.launcher.ducc_spawn_path')

        self.ducc_uid          = find_ducc_uid()
        # self.broker_url      = self.ducc_properties.get('ducc.broker.url')
        self.broker_protocol   = self.ducc_properties.get('ducc.broker.protocol')
        self.broker_host       = self.localhost
        self.broker_port       = self.ducc_properties.get('ducc.broker.port')
        self.broker_jmx_port   = self.ducc_properties.get('ducc.broker.jmx.port')
        self.broker_decoration = self.ducc_properties.get('ducc.broker.url.decoration')
        self.broker_url        = self.broker_protocol + '://' + self.broker_host + ':' + self.broker_port
        self.agent_jvm_args    = self.ducc_properties.get('ducc.agent.jvm.args')
        self.ws_jvm_args       = self.ducc_properties.get('ducc.ws.jvm.args')
        self.pm_jvm_args       = self.ducc_properties.get('ducc.pm.jvm.args')
        self.rm_jvm_args       = self.ducc_properties.get('ducc.rm.jvm.args')
        self.sm_jvm_args       = self.ducc_properties.get('ducc.sm.jvm.args')
        self.or_jvm_args       = self.ducc_properties.get('ducc.orchestrator.jvm.args')


        if ( self.broker_decoration == '' ):
            self.broker_decoration = None

        if ( self.broker_decoration != None ):
            self.broker_url = self.broker_url + '?' + self.broker_decoration
        
        if ( self.webserver_node == None ):
            self.webserver_node = self.localhost

    def merge_properties(self):
        # first task, always, merge the properties so subsequent code can depend on their validity.
        base_props = DUCC_HOME + '/resources/default.ducc.properties'
        site_props = DUCC_HOME + '/resources/site.ducc.properties'
        run_props = DUCC_HOME + '/resources/ducc.properties'
        merger = DUCC_HOME + '/admin/ducc_props_manager'
        CMD = [merger, '--merge', base_props, '--with', site_props, '--to', run_props]
        CMD = ' '.join(CMD)
        print 'Merging', base_props, 'with', site_props, 'into', run_props
        os.system(CMD)

    def check_properties(self):
        database_host_list = self.ducc_properties.get('ducc.database.host.list')
        database_host = self.ducc_properties.get('ducc.database.host')
        database_jmx_host = self.ducc_properties.get('ducc.database.jmx.host')
        if(database_host_list != None):
            if(database_host != None):
                text = 'ducc.database.host_list and ducc.database.host both specified.'
                print 'Error: '+text
                sys.exit(1)
            if(database_jmx_host != None):
                if(database_jmx_host == 'localhost'):
                    pass
                elif(database_jmx_host in database_host_list.split()):
                    pass
                else:
                    text = 'ducc.database.host_list and ducc.database.jmx.host both specified.'
                    print 'Error: '+text
                    sys.exit(1)
    
    def get_db_host_list(self):
        result = []
        slist = self.ducc_properties.get('ducc.database.host.list')
        if(slist == None):
            slist = self.ducc_properties.get('ducc.database.host')
        if(slist != None):
            result = slist.split()
        return result
    
    def get_db_host(self):
        result = None
        host_list = self.get_db_host_list()
        if(host_list != None):
            if(len(host_list) > 0):
                if(result == None):
                    this_host = self.get_hostname_long()
                    if(this_host in host_list):
                        result = this_host
                if(result == None):
                    this_host = self.get_hostname_short()
                    if(this_host in host_list):
                        result = this_host
                if(result == None):
                    result = host_list[0]
        return result
    
    def is_db_disabled(self):
        result = False
        dbhost = self.get_db_host()
        if(dbhost == self.db_disabled):
            result = True
        return result
    
    def db_configure(self):
        if(self.is_db_disabled()):
            self.db_bypass = True
            return;
        else:
            self.db_bypass = False

        dbprops = Properties()
        dbprops.load(self.DUCC_HOME + '/resources.private/ducc.private.properties')
        self.db_password = dbprops.get('db_password')
        if ( self.db_password == None ):
            print "bypassing database because no password is set."
            self.db_bypass = True

        self.db_password_guest = dbprops.get('db_password_guest')
        if ( self.db_password_guest == None ):
            self.db_password_guest = 'guest'
    
    def db_password(self):
        if(self.db_password == None):
            self.db_configure()
        return self.db_password
    
    def db_password_guest(self):
        if(self.db_password == None):
            self.db_configure()
        return self.db_password_guest
    
    # does the database process exist?  
    def db_process_alive(self):
        if ( not os.path.exists(self.db_pidfile) ):
            return False

        f = open(self.db_pidfile)
        pid = f.read();
        f.close()
        answer = []
        if ( self.system == 'Darwin'):
            ps = 'ps -eo user,pid,comm,args ' + pid
        else:
            ps = 'ps -eo user:14,pid,comm,args ' + pid
        lines = self.popen(ps)
        
        for line in lines:
            line = line.strip()
            if (pid in line and 'cassandra' in line):
                return True
        return False

    def db_process_kill(self,code):
        if ( not os.path.exists(self.db_pidfile) ):
            return False

        f = open(self.db_pidfile)
        pid = f.read();
        f.close()
        answer = []
        if ( self.system == 'Darwin'):
            ps = 'ps -eo user,pid,comm,args ' + pid
        else:
            ps = 'ps -eo user:14,pid,comm,args ' + pid
        lines = self.popen(ps)
        
        for line in lines:
            line = line.strip()
            if (pid in line and 'cassandra' in line):
                cmd = [ 'kill', '-'+str(code), pid ]
                cmd = ' '.join(cmd)
                rc = os.system(cmd)
                if ( rc == 0 ):
                    return True
                else:
                    return False
        return False

    # contact the database and see how useful it seems to be
    def db_alive(self, retry=10, verbose=True):
        if ( self.db_bypass == True ):
            return True
        else:
            return self.db_alive_check(retry,verbose)
        
    def db_alive_check(self, retry=10, verbose=True):
        if(self.is_db_disabled()):
            if(verbose):
                print 'No database location defined.'
            return False

        if ( not os.path.exists(self.db_pidfile) ):
            if(verbose):
                print 'Database pid file does not exist.  Checking DB connectivity.'

        # get our log4j config into the path to shut up noisy logging
        os.environ['CLASSPATH'] = os.environ['CLASSPATH'] + ':' + self.DUCC_HOME + '/resources'
        
        dbnode = self.get_db_host()
        
        CMD = [self.java(), 'org.apache.uima.ducc.database.DbAlive', self.DUCC_HOME, dbnode, str(retry)]

        CMD = ' '.join(CMD)
        if(not verbose):
            CMD = CMD + " >/dev/null 2>&1" 
        rc = os.system(CMD)
        if ( rc == 0 ):
            return True
        else:
            return False

            
    def db_start(self):

        if(not self.automanage_database):
            print '   (Bypass database start - not automanaged)'
            return False

        # bypass all of this for the initial delivery
        if ( self.db_bypass == True) :
            print '   (Bypass database start)'
            return True
        
        dbnode = self.get_db_host()
        
        if(dbnode == None):
            print '   (Bypass database start - no database configured)'
            return False
        
        dblist = self.get_db_host_list()
        if(len(dblist) > 1):
            print '   (Bypass database start - database list not supported)'
            return True
        
        print 'Starting database'
        dbu.update_cassandra_config(self.DUCC_HOME, dbnode)

        max_attempts = 5
        attempt = 1
        while attempt <= max_attempts:
            lines = self.ssh(dbnode, True, "'", self.DUCC_HOME + '/admin/ducc.py', '-c', 'db', '--nodup', "'")
            # we'll capture anything that the python shell spews because it may be useful, and then drop the
            # pipe when we see a PID message

            while True:
                try:
                    line = lines.readline().strip()
                except:
                    break
                #print '[]', line
                
                if ( not line ):
                    break
                if ( line == '' ):
                    break
                if ( line == 'Database is already running.' ):
                    print 'Database process is running ... if connection fails stop the DB and try again'
                    break
                if ( line == 'OK' ):
                    print 'GOT OK from db start'
                    lines.close();

            print 'waiting for database to start'
            if ( self.db_alive() ):
                return True

            attempt = attempt + 1
            if attempt <= max_attempts:
                print 'Did not connect to database, retrying (', attempt, 'of', max_attempts, ')'

        return False

    def db_stop(self):
        try:
            if(not self.automanage_database):
                print '   (Bypass database start - not automanaged)'
                return False
            if ( self.db_bypass == True) :
                print '   (Bypass database stop)'
                return True
            dbnode = self.get_db_host()
            if(dbnode == None):
                print '   (Bypass database stop - no database configured)'
                return False
            dblist = self.get_db_host_list()
            if(len(dblist) > 1):
                print '   (Bypass database stop - database list not supported)'
                return True
            dbnode = dbnode.strip()
            pidfile = os.path.join(DUCC_HOME,'state','database',dbnode,'cassandra.pid')
            cmd = [ 'less', '-FX', pidfile ]
            cmd = ' '.join(cmd)
            #print cmd
            stdout = self.ssh(dbnode, True, cmd)
            result = stdout.read().strip()
            tokens = result.split()
            if(len(tokens) == 1):
                pid = tokens[0].strip()
                #print pid
                cmd = [ 'kill', '-15', pid ]
                cmd = ' '.join(cmd)
                #print cmd
                stdout = self.ssh(dbnode, True, cmd)
                result = stdout.read().strip()
                #print result
                print 'Database stopped.'
                return True
            else:
                #print result
                print 'Database not running.'
                return True
        except Exception,e:
            print e
            return False

    def find_netstat(self):
        # don't you wish people would get together on where stuff lives?
        if ( os.path.exists('/sbin/netstat') ):
            return '/sbin/netstat'
        if ( os.path.exists('/usr/sbin/netstat') ):
            return '/usr/sbin/netstat'
        if ( os.path.exists('/bin/netstat') ):
            return '/bin/netstat'
        if ( os.path.exists('/sbin/netstat') ):
            return '/usr/bin/netstat'
        print 'Cannot find netstat'
        return None

    def is_amq_active(self):
        netstat = self.find_netstat()
        if ( netstat == None ):
            print "Cannot determine if ActiveMq broker is alive."
            return false

        lines = self.ssh(self.broker_host, True, netstat, '-an')
        #
        # look for lines like this with the configured port in the 4th token, and
        # ending with LISTEN:
        #
        # tcp        0      0 :::61616                :::*                    LISTEN      
        for line in lines:
            toks = line.split()
            #print '[]', line
            if ( toks[-1] == 'LISTEN' ):
                port = toks[3]
                if (port.endswith(self.broker_port)):
                    return True
        return False        

    def stop_broker(self):

        broker_host = self.localhost
        broker_home = self.ducc_properties.get('ducc.broker.home')
        broker_name = self.ducc_properties.get('ducc.broker.name')
        broker_jmx  = self.ducc_properties.get('ducc.broker.jmx.port')
        here = os.getcwd()
        CMD = broker_home + '/bin/activemq'
        CMD = CMD + ' stop --all'
        CMD = CMD + ' --jmxurl service:jmx:rmi:///jndi/rmi://' + broker_host + ':' + broker_jmx + '/jmxrmi' 
        CMD = CMD + ' ' + broker_name
        CMD = 'JAVA_HOME=' + self.java_home() + ' ' + CMD
        print '--------------------', CMD
        lines = self.ssh(broker_host, True, CMD)
        for l in lines:
            pass       # throw away junk from ssh


    def nohup(self, cmd, showpid=True):
        # Skip use of ssh?
        if cmd[0] == "ssh" and 'false' == self.ssh_enabled:
            cmd = cmd[2:]
        cmd = ' '.join(cmd)
        # print '**** nohup', cmd, '****'
        devnw = open(os.devnull, 'w')
        devnr = open(os.devnull, 'r')
        ducc = subprocess.Popen(cmd, shell=True, stdin=devnr, stdout=devnw, stderr=devnw)
        devnr.close()
        devnw.close()
        if ( showpid ) :
            print 'PID', ducc.pid

    def get_hostname_long(self):
        hostname = '?'
        cmd = '/bin/hostname'
        resp = self.popen(cmd)
        lines = resp.readlines()
        if(len(lines)== 1):
            line = lines[0]
            hostname = line.strip();
        return hostname
    
    def get_hostname_short(self):
        hostname = '?'
        cmd = '/bin/hostname -s'
        resp = self.popen(cmd)
        lines = resp.readlines()
        if(len(lines)== 1):
            line = lines[0]
            hostname = line.strip();
        return hostname

    def get_hostname(self):
        hostname = '?'
        cmd = '/bin/hostname'
        resp = self.popen(cmd)
        lines = resp.readlines()
        if(len(lines)== 1):
            line = lines[0]
            line = line.strip();
            hostname = line.split('.')[0]
        return hostname
    
    def ssh_operational(self, node, verbosity=True):
        is_operational = False
        req = node.split('.')[0]
        cmd = '/bin/hostname'
        if(node == 'localhost'):
            req = self.get_hostname()
        ssh_cmd = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=10'+' '+node+" "+cmd
        resp = self.popen(ssh_cmd)
        lines = resp.readlines()
        for line in lines:
            if(node in line):
                return True
        print 'not found: ', node
        for line in lines:
            print line
        if(verbosity):
            print 'ssh not operational - unexpected results from:', ssh_cmd
            for line in lines:
                print '>>>>>',line
        return False

    # like popen, only it spawns via ssh
    # Skip use of ssh?
    # NOTE: Current callers always have do_wait True
    def ssh(self, host, do_wait, *CMD):
        cmd = ' '.join(CMD)
                # Some callers quote the string which is OK for ssh but not the direct call
        if cmd[0] == "'" and cmd[-1] == "'":
            cmd = cmd[1:len(cmd)-2]
        if ( do_wait ):
            if 'false' == self.ssh_enabled:
                return self.popen(cmd)
            return self.popen('ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=10', host, cmd)
        else:
            if 'false' == self.ssh_enabled:
                return self.spawn(cmd)
            return self.spawn('ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=10', host, cmd)


    def set_classpath(self):
        DH        = self.DUCC_HOME + '/'
        LIB       = DH + 'lib/'

        local_jars  = self.ducc_properties.get('ducc.local.jars')   #local mods
    
        CLASSPATH = ''
    
        if ( local_jars != None ):
            extra_jars = local_jars.split()
            for j in extra_jars:
                CLASSPATH = CLASSPATH + ':' + LIB + j

        CLASSPATH = CLASSPATH + ':' + DH  + 'apache-uima/lib/*'           
        CLASSPATH = CLASSPATH + ':' + DH  + 'apache-uima/apache-activemq/lib/*'           
        CLASSPATH = CLASSPATH + ':' + DH  + 'apache-uima/apache-activemq/lib/optional/*'           
        CLASSPATH = CLASSPATH + ':' + LIB + 'apache-commons/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'guava/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'google-gson/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'apache-log4j/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'apache-camel/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'joda-time/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'springframework/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'jna/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'libpam4j/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'uima-ducc/*'
        CLASSPATH = CLASSPATH + ':' + LIB + 'cassandra/*'

        # CLASSPATH = CLASSPATH + ':' + DH  + 'resources'  UIMA-4168 Use API, not classpath to configure log4j
    
        # more are added to some components in ducc.py, e.g.
        #    apache-activemq/lib/optional, jetty from ws lib, jsp, http- client
        #   
        os.environ['CLASSPATH'] = CLASSPATH

    def format_classpath(self, cp):
        strings = cp.split(':')
        for s in strings:
            print s

    def check_clock_skew(self, localdate):
        user = os.environ['LOGNAME']
        bypass = (user != self.ducc_uid)
        
        if bypass:
            tag = 'NOTE'
        else:
            tag = 'NOTOK'

        # Check clock skew
        ok = True
        acceptable_skew = 300
        skew = abs(long(localdate) - long(time.time()))
        if ( skew > (acceptable_skew) ):
            ok = False
            print tag, 'Clock skew[', skew, '] on', os.uname()[1], ". Remote time is", time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime())
        return ok or bypass

    def set_duccling_version(self):
        CMD = self.duccling + ' -v >' + self.DUCC_HOME + '/state/duccling.version'
        os.system(CMD)
        print 'Set ducc_ling version from', self.localhost, ':', CMD

    def verify_limits(self):
        ret = True
        if ( self.system == 'Darwin' ):
            return ret                 # on mac, just use what you have

        proclimit = 20000
        filelimit = 8192

        (softnproc , hardnproc)  = resource.getrlimit(resource.RLIMIT_NPROC)
        (softnfiles, hardnfiles)  = resource.getrlimit(resource.RLIMIT_NOFILE)
        if ( softnproc < hardnproc ):
            try:
                resource.setrlimit(resource.RLIMIT_NPROC, (hardnproc, hardnproc))
            except:
                print 'NOTOK: could not set soft RLIMIT_NPROC up to the hard limit'
                ret = False

        if ( softnfiles < hardnfiles ):
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hardnfiles, hardnfiles))
            except:
                print 'NOTOK: could not set soft RLIMIT_NOFILES up to the hard limit'
                ret = False

        (softnproc , hardnproc)  = resource.getrlimit(resource.RLIMIT_NPROC)
        (softnfiles, hardnfiles)  = resource.getrlimit(resource.RLIMIT_NOFILE)
        
        if ( softnproc < proclimit ):
            print 'WARN: Soft limit RLIMIT_NPROC is too small at', softnproc, '(<', proclimit, ' ).  DUCC may be unable to create sufficient threads.'

        if ( softnfiles < filelimit ):
            print 'WARN: Soft limit RLIMIT_NOFILES is too small at', softnfiles, '(<', filelimit, ').  DUCC may be unable to open sufficient files or sockets.'

        return ret

    def verify_jvm(self):
        jvm = self.java()
        CMD = jvm + ' -version > /dev/null 2>&1'
        rc = os.system(CMD)
        if ( rc != 0 ):
            print 'NOTOK', CMD, 'returns', int(rc), '.  Must return rc 0.  Startup cannot continue.'
            return False
        return True
    
    # determine if string represent an integer
    def is_int(self,string):
        result = True
        try:
            number = int(string)
        except:
            result = False
        return result
    
    # transform hostname into ip address
    def get_ip_address(self,hostname):
        label = 'get_ip_address'
        result = None
        try:
            # get virtual ip address from keepalived.conf
            result = self.get_virtual_ipaddress()
            if(result == None):
                # get virtual ip address from nameserver
                p = subprocess.Popen(['/usr/bin/nslookup', hostname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output, err = p.communicate()
                #print hostname, output, err
                name = None
                for line in output.splitlines():
                    tokens = line.split()
                    if(len(tokens) == 2):
                        t0 = tokens[0]
                        t1 = tokens[1]
                        if(t0 == 'Address:'):
                            if(name != None):
                                result = t1
                                break
                        elif(t0 == 'Name:'):
                            name = t1
        except Exception as e:
            print e
        debug(label, str(result))
        return result
    
    # get ducc.head.reliable.list
    def get_head_node_list(self):
        head_node_list = []
        # add ducc.head.reliable.list node(s)
        ducc_head_list = self.ducc_properties.get("ducc.head.reliable.list")
        if(ducc_head_list != None):
            ducc_head_nodes = ducc_head_list.split()
            if(len(ducc_head_nodes)== 0):
                pass
            elif(len(ducc_head_nodes)== 1):
                print '>>> ERROR - "ducc.head.reliable.list" missing or invalid.'
                sys.exit(1);
            else:
                head_node_list = ducc_head_nodes
        return head_node_list
    
    # get all possible hostnames & ip addresses for a head node
    def get_head_node_list_variations(self):
        # start with ducc.head.reliable.list node(s)
        head_node_list = self.get_head_node_list()
        # add ducc.head node
        ducc_head = self.ducc_properties.get("ducc.head")
        if(ducc_head == None):
            print '>>> ERROR - "ducc.head" missing or invalid.'
            sys.exit(1);
        ducc_head_nodes = ducc_head.split()
        if(len(ducc_head_nodes) != 1):
            print '>>> ERROR - "ducc.head" missing or invalid.'
            sys.exit(1);
        head_node = ducc_head_nodes[0]
        if(not head_node in head_node_list):
            head_node_list.append(head_node)
        # add short names
        list = head_node_list
        for node in list:
            short_name = node.split('.')[0]
            if(not self.is_int(short_name)):
                if(not short_name in head_node_list):
                    head_node_list.append(short_name)
        # add ip addresses
        list = head_node_list
        for node in list:
            ip = self.get_ip_address(node)
            if(ip != None):
                if(not ip in head_node_list):
                    head_node_list.append(ip)
        #
        debug('head_node_list: ', head_node_list)
        return head_node_list
    
    # drop domain and whitespace
    def normalize(self,name):
        result = name
        if(name != None):
            result = name
            result = result.strip()
            result = result.split('.')[0]
        return result
    
    # get current host's name
    def get_node_name(self):
        node_name = 'unknown'
        cmd = '/bin/hostname'
        resp = self.popen(cmd)
        lines = resp.readlines()
        if(len(lines)== 1):
            name = lines[0]
            node_name = self.normalize(name)
        debug('node_name: ', node_name)
        return node_name
    
    def is_head_node(self):
        retVal = False
        head_node_list = self.get_head_node_list_variations()
        node = self.get_node_name()
        if(node in head_node_list):
            retVal = True
        return retVal
    
    # Exit if this is not the head node.  Ignore the domain as uname sometimes drops it.
    # Also check that ssh to this node works
    # Also restrict operations to the userid that installed ducc
    def verify_head(self):
        head_node_list = self.get_head_node_list_variations()
        node = self.get_node_name()
        if(node in head_node_list):
            pass
        else:
            ip = self.get_ip_address(node)
            if(ip in head_node_list):
                pass
            else:
                print ">>> ERROR - "+node+" not configured as head node."
                sys.exit(1);
        if(self.ssh_operational(node)):
            text = "ssh is operational to "+node
            #print text
        else:
            print ">>> ERROR - this script cannot ssh to head node"
            sys.exit(1);
        # Ensure that root or another id doesn't start/stop ducc
        dir_stat = os.stat(DUCC_HOME + '/resources/site.ducc.properties')
        if dir_stat.st_uid != os.getuid():
            print ">>> ERROR - this script must be run by the userid that installed DUCC"
            sys.exit(1);

    #
    # Verify the viability of ducc_ling.
    # Returns a tuple (viable, elevated, safe)
    #   where 'viable' is a boolean indicating whether the ducc_ling is viable (exists and is correct version)
    #            If this is false, the other two values are meaningless
    #
    #         'elevated' indicates whether priveleges are at least partially elevated
    #         'safe' indicates whether ducc_ling is safe (elevated privileges and correct permissions)
    # The caller will evaluate these and take appriopriate action
    #
    def verify_duccling(self):
        viable = True
        elevated = False
        safe = False

        dl   = self.duccling
        path = os.path.dirname(os.path.abspath(dl))       

        
        if ( not (os.path.exists(dl) and os.access(dl, os.X_OK)) ):
            print dl, 'does not exist or is not executable.'
            viable = False

        path = os.path.dirname(os.path.abspath(dl))
        dl   = path + '/ducc_ling'
        
        dl_stat = os.stat(dl)          # dl_stat is stat for ducc_ling
        dl_mode = dl_stat.st_mode

        if ( (dl_mode & S_ISUID) != S_ISUID):
            if ( os.environ['LOGNAME'] == self.ducc_uid ):
                print 'ducc_ling module', dl, ': setuid bit is not set. Processes will run as user '+self.ducc_uid
            elevated = False
            file_safe = True
            dir_safe  = True
            own_safe  = True
        else:
            elevated = True
            file_safe = False
            dir_safe  = False
            own_safe  = False


        if ( elevated ):

            #
            # if setuid bit is set, all this MUST be true or we won't mark ducc_ling safe:
            #    file permissions are 750
            #    dir  permissions are 700
            #    owenership is root.ducc
            #
            dl_perm = oct(dl_mode & (S_IRWXU | S_IRWXG | S_IRWXO))
            expected = oct(0750)
            if ( dl_perm != expected ):
                 print dl, ': Invalid execution bits', dl_perm, 'should be', expected
            else:
                file_safe = True

            dir_stat = os.stat(path)          # dir_stat is stat for ducc_ling
            dir_mode = dir_stat.st_mode

            dir_perm = oct(dir_mode & (S_IRWXU | S_IRWXG | S_IRWXO))
            expected = oct(0700)
            if ( dir_perm != expected ):
                 print 'ducc_ling', path, ': Invalid directory permissions', dir_perm, 'should be', expected
            else:
                 dir_safe = True

            try:
                grpinfo = grp.getgrnam(self.ducc_uid)
                duccgid = grpinfo.gr_gid

                if ( (dl_stat.st_uid != 0) or (dl_stat.st_gid != duccgid) ):
                    print 'ducc_ling module', dl, ': Invalid ownership. Should be '+self.ducc_uid
                else:
                    own_safe = True
            except:
                print 'ducc_ling group "'+self.ducc_uid+'" cannot be found.'

        safe = file_safe and dir_safe and own_safe

        # A last viability check, do versions match? This also runs it, proving it
        # can execute in this environment.
        lines = self.popen(self.duccling + ' -v')
        version_from_head = lines.readline().strip();
        toks = version_from_head.split()
        version_from_head = ' '.join(toks[0:4])

        version_file = self.DUCC_HOME + '/state/duccling.version';
        if ( os.path.exists(version_file) ):
            verfile = open(version_file)
            for line in verfile:
                line = line.strip();
                toks = line.split();
                line = ' '.join(toks[0:4])
                if ( line != version_from_head ):
                    print "Mismatched ducc_ling versions:"
                    print "ALERT: Version on Agent Node:", version_from_head
                    print "ALERT: Version on Ducc  Head:", line
                    viable = False
            verfile.close()
        else:
            print "NOTE: ducc_ling version file missing, cannot verify version."

        # leave the decisions to the caller
        return (viable, elevated, safe)        

    # Apply these rules to determine if ducc_ling is installed ok
    #
    #    Caller            Elevated         Protected (safe)     Action
    #    --------          --------         ---------            ------
    #    ducc                 Y                 Y                OK
    #                         Y                 N                Fail
    #
    #                         N                 Y (by def)       OK
    #
    #    ~ducc                Y                 N (by def)       Fail
    #                         N                 Y (by def)       OK, Note    
    def duccling_ok(self, viable, elevated, safe):

        if ( not viable ):
            return False

        user = os.environ['LOGNAME']
        
        if ( user == self.ducc_uid ):
            if ( elevated ):
                return safe
        else:
            if ( elevated ):
                return False
            print 'Note: Running unprivileged ducc_ling. Process will run as user', user

        return True

    def ssh_ok(self, node, line):
        spacer = '   '
        messages = []
        if ( line.startswith("Permission denied") ):
            messages.append(' ')
            messages.append(spacer + "ALERT: Passwordless SSH is not configured correctly for node " + node)
            messages.append(spacer + "ALERT: SSH returns '" + line + "'")
            return messages

        if ( line.startswith("Host key verification failed") ):
            messages.append(' ')
            messages.append(spacer + "ALERT: Passwordless SSH is not configured correctly for node " + node)
            messages.append(spacer + "ALERT: SSH returns '" + line + "'")
            return messages

        if ( line.find("Connection refused") >= 0 ):
            messages.append(' ')
            messages.append(spacer + "ALERT: SSH is not not enabled on node " + node)
            messages.append(spacer + "ALERT: SSH returns '" + line + "'")
            return messages
        
        if ( line.find("Connection timed") >= 0 ):
            messages.append(' ')
            messages.append(spacer + "\nALERT: SSH did not respond with timeout of 10 secnds " + node)
            messages.append(spacer + "ALERT: SSH returns '" + line + "'")
            return messages
        
        if ( line.find("No route")  >= 0 ):
            messages.append(' ')
            messages.append(spacer + 'ALERT: SSH cannot connect to host.')
            messages.append(spacer + "ALERT: SSH returns '" + line + "'")
            return messages

        return None

        
    #
    # Input is array lines from ps command looking for ducc processes owned this user.
    # Output is list of dictionaries, where each dictionary describes a ducc process.
    #
    # If no ducc processes are found here the list is empty.
    #
    # The caller executes the 'ps' command and knows the node this is for.
    #
    def find_ducc_process(self, node):
    
        answer = []
        if ( self.system == 'Darwin'):
            ps = 'ps -eo user,pid,comm,args'
        else:
            ps = 'ps -eo user:14,pid,comm,args'
        resp = self.ssh(node, True, ps)
        ok = True

        while True:
            line = resp.readline().strip()           
            if ( line.startswith('PID')):
                continue

            ssh_errors = self.ssh_ok(line, node)
            if ( ssh_errors != None ):
                for m in ssh_errors:
                    print m
                ok = False
                continue

            # from here on, assume no error
            if ( not line ):
                break
            
            toks = line.split()
            if ( len(toks) < 4):
                continue

            user = toks[0]
            pid = toks[1]
            procname = toks[2]
            fullargs = toks[3:]

            if ( not ('java' in procname) and not ('JIT' in procname)):
                continue

            cont = False
            for tok in fullargs:
                if ( tok.startswith('-Dducc.deploy.components=') ):
                    cmp = tok.split('=')
                    dp = (cmp[1],  pid, user)
                    answer.append(dp)
                    cont = True
                    break
                if ( tok.startswith('-DDUCC_BROKER_CREDENTIALS_FILE=') ):
                    dp = ('broker',  pid, user)
                    answer.append(dp)
                    cont = True
                    break
            if ( cont ):             # stupid python only continues out of inner loop
                continue
            if fullargs[-1] == 'org.apache.cassandra.service.CassandraDaemon':
                dp = ('database',  pid, user)
                answer.append(dp)
                continue
            
            # Look for site-specific processes
            other_processes = find_other_processes(pid, user, line)
            if ( type(other_processes) is list ):
                if ( len(other_processes) > 0 ):
                    answer = answer + other_processes
            else:
                print 'Invalid response from \'find_other_processes\':', other_processes

        return (ok, answer)

    #
    # Given the name of a file containing ducc nodes, a ducc user (usually the 'ducc' user unless you're running
    #   as yourself for test), find all ducc processes owned by this user and print them to the console.
    #
    def find_ducc(self, nodefile, user):
        if ( nodefile == None ):
            nodefile = self.DUCC_HOME + '/resources/ducc.nodes'
    
        if ( not os.path.exists(nodefile) ):
            print 'Nodefile', nodefile, 'does not exist or cannot be read.'
            sys.exit(1)
    
        answer = {}
        nodes = []
        f = open(nodefile)
        for node in f:
            node = node.strip()
            if ( not node ):
                continue
            if ( node.startswith('#') ):
                continue
            nodes.append(node)

        if ( self.webserver_node != 'localhost' ):           # might be configured somewhere else
            nodes.append(self.webserver_node)

        for node in nodes:                
            data = self.find_ducc_process(node, user)
            answer[node] = data

        return answer

    def kill_process(self, node, proc, signal):
        lines = self.ssh(node, True, 'kill', signal, proc[1])
        for l in lines:
            pass # throw away the noise
                
    def clean_shutdown(self):
        DUCC_JVM_OPTS = ' -Dducc.deploy.configuration=' + self.DUCC_HOME + "/resources/ducc.properties "
        DUCC_JVM_OPTS = DUCC_JVM_OPTS + ' -DDUCC_HOME=' + self.DUCC_HOME
        DUCC_JVM_OPTS = DUCC_JVM_OPTS + ' -Dducc.head=' + self.ducc_properties.get('ducc.head')
        self.spawn(self.java(), DUCC_JVM_OPTS, 'org.apache.uima.ducc.common.main.DuccAdmin', '--killAll')

    def ducc_admin(self,option,target):
        DUCC_JVM_OPTS = ' -Dducc.deploy.configuration=' + self.DUCC_HOME + "/resources/ducc.properties "
        DUCC_JVM_OPTS = DUCC_JVM_OPTS + ' -DDUCC_HOME=' + self.DUCC_HOME
        DUCC_JVM_OPTS = DUCC_JVM_OPTS + ' -Dducc.head=' + self.ducc_properties.get('ducc.head')
        java_class = 'org.apache.uima.ducc.common.main.DuccAdmin'
        cmd = self.java()+' '+DUCC_JVM_OPTS+' '+java_class+' '+option+' '+target
        print cmd
        self.spawn(self.java(), DUCC_JVM_OPTS, java_class, option, target)
        
    def get_os_pagesize(self):
        lines = self.popen('/usr/bin/getconf', 'PAGESIZE')
        return lines.readline().strip()

    def show_ducc_environment(self):
        global use_threading
        #
        # Print the java version
        #
        response = []
        jvm = self.ducc_properties.get('ducc.jvm')
        jvm = jvm.replace('//', '/')
        check_java = True
        if ( jvm == None ):
            response.append('WARNING: No jvm configured.  Default is used.')
            jvm = 'java'
        else:
            response.append('ENV: Java is configured as: ' + jvm)
            if ( not os.path.exists(jvm) ):
                print 'NOTOK: configured jvm cannot be found:', jvm
                check_java = False

        if ( check_java ):
            print 'JVM:', jvm
            lines = self.popen(jvm + ' -fullversion')
            for line in lines:
                response.append('ENV ' + line.strip())
                

        response.append('ENV: Threading enabled: ' + str(use_threading))
        #
        # Get the total memory for the node
        #
        if ( self.system != 'Darwin' ):
            meminfo = Properties()
            meminfo.load('/proc/meminfo')
            mem = meminfo.get('MemTotal')
            if ( mem.endswith('kB') ):
                toks = mem.split(' ')
                mem = str(int(toks[0]) / (1024*1024)) + ' gB'
                response.append('MEM: memory is ' + mem)

        #
        # Get the operating system information
        #
        response.append('ENV: system is ' + self.system)

        return response

    #
    # Resolve the 'path' relative to the path 'relative_to'
    #
    def resolve(self, path, relative_to):
        if ( not path.startswith('/') ):                        
            (head, tail) = os.path.split(os.path.abspath(relative_to))
            path = head + '/' + path
        return path

    #
    # Read the nodefile, recursing into 'imports' if needed, returning a
    # map.  The map is keyed on filename, with each entry a list of the nodes.
    # Skip file with suffix ".regex".
    #
    def read_nodefile(self, rnodefile, ret):
        #print 'READ_NODEFILE:', nodefile, ret
        n_nodes = 0
        if(rnodefile.startswith('/')):
            nodefile = rnodefile
        else:
            nodefile = os.path.join(base_dir(),'resources',rnodefile)
        #print nodefile
        if(nodefile.endswith('.regex')):
            pass
        elif ( os.path.exists(nodefile) ):
            nodes = []
            f = open(nodefile)
            for node in f:
                node = node.strip()
                if ( not node ):
                    continue
                if ( node.startswith('#') ):
                    continue
                if ( node.startswith('import ') ):
                    toks = node.split(' ')
                    newfile = toks[1]
                    newfile = self.resolve(newfile, nodefile)  # resolve newfile relative to nodefile
                    (count, ret) = self.read_nodefile(newfile, ret)
                    n_nodes = n_nodes + count
                    continue
                nodes.append(node)
                n_nodes = n_nodes + 1
            ret[nodefile] = nodes
        else:
            n_nodes = -1
            print 'Cannot read nodefile', nodefile
            ret[nodefile] = None

        #print 'RETURN', n_nodes, nodefile, ret
        return (n_nodes, ret)

    def compare_nodes(self, n1, n2):

        if ( n1 == n2 ):             # exact match - covers both short and both long
            return True

        if ( n1.find('.') >= 0 ):    # shortened n1 == n2?
            t1 = n1.split('.')
            n1A = t1[0]
            if ( n1A == n2 ):
                return True

        if ( n2.find('.') >= 0 ):    # n1 == shortened n2?
            t2 = n2.split('.')
            n2A = t2[0]
            if ( n1 == n2A ):
                return True
        return False

    def verify_class_configuration(self, allnodes, verbose):

        print 'allnodes', allnodes
        answer = True
        # first, find the class definition
        classfile = self.ducc_properties.get('ducc.rm.class.definitions')

        print 'Class definition file is', classfile
        CMD = self.jvm
        CMD = CMD + " -DDUCC_HOME=" + self.DUCC_HOME
        CMD = CMD + " org.apache.uima.ducc.common.NodeConfiguration "
        CMD = CMD + " -n " + allnodes
        CMD = CMD + " -c " + classfile
        if ( verbose ):
            CMD = CMD + " -p " + classfile
            print CMD
        else:
            CMD = CMD + " " + classfile
        rc = os.system(CMD)
        if ( rc == 0 ):
            print "OK: Class and node definitions validated."
        else:
            print "NOTOK: Cannot validate class and/or node definitions."

        return (rc == 0)

    def get_nodepool(self, node, default=''):
        classpath = '"'+self.DUCC_HOME+'/lib/uima-ducc/*'+'"'
        #print classpath
        classfile = self.ducc_properties.get('ducc.rm.class.definitions')
        #print 'classfile: '+classfile
        cmd = ''
        cmd = cmd+self.jvm
        cmd = cmd + ' '
        cmd = cmd+'-cp '+classpath
        cmd = cmd + ' '
        cmd = cmd+'-DDUCC_HOME='+self.DUCC_HOME
        cmd = cmd + ' '
        cmd = cmd+'org.apache.uima.ducc.common.NodeConfiguration'
        cmd = cmd + ' '
        cmd = cmd+'-c'+' '+classfile
        cmd = cmd + ' '
        cmd = cmd+'-m'+' '+node
        cmd = ''.join(cmd)
        #print 'cmd: '+cmd
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        (out, err) = p.communicate()
        status = p.wait()
        result = out.strip()
        if(result == ''):
            result = default
        #print 'result: '+result
        #print 'status: '+str(status)
        return result

    def get_nodepool_file(self, node, default=''):
        classpath = '"'+self.DUCC_HOME+'/lib/uima-ducc/*'+'"'
        #print classpath
        classfile = self.ducc_properties.get('ducc.rm.class.definitions')
        #print 'classfile: '+classfile
        cmd = ''
        cmd = cmd+self.jvm
        cmd = cmd + ' '
        cmd = cmd+'-cp '+classpath
        cmd = cmd + ' '
        cmd = cmd+'-DDUCC_HOME='+self.DUCC_HOME
        cmd = cmd + ' '
        cmd = cmd+'org.apache.uima.ducc.common.NodeConfiguration'
        cmd = cmd + ' '
        cmd = cmd+'-c'+' '+classfile
        cmd = cmd + ' '
        cmd = cmd+'-m'+' '+node
        cmd = cmd + ' '
        cmd = cmd+'-f'+' '+node
        cmd = ''.join(cmd)
        #print 'cmd: '+cmd
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        (out, err) = p.communicate()
        status = p.wait()
        result = out.strip()
        if(result == ''):
            result = default
        #print 'result: '+result
        #print 'status: '+str(status)
        return result
        
    def disable_threading(self):
        global use_threading
        use_threading = False

    def installed(self):
        head = self.ducc_properties.get('ducc.head')
        if ( head == '<head-node>' ):
            return False
        return True

    keepalived_conf = '/etc/keepalived/keepalived.conf'

    def get_virtual_ipaddress(self):
        state = 0
        vip = None
        if ( os.path.exists(self.keepalived_conf) ):
            with open(self.keepalived_conf) as f:
                for line in f:
                    tokens = line.split(' ')
                    if(tokens == None):
                        pass
                    elif(len(tokens) == 0):
                        pass
                    elif(tokens[0] == '!'):
                        pass
                    else:
                        for token in tokens:
                            token = token.strip()
                            if(len(token) == 0):
                                continue
                            elif(token == '#'):
                                break
                            if(state == 0):
                                if(token == 'virtual_ipaddress'):
                                    state = 1
                            elif(state == 1):
                                if(token == '{'):
                                    state = 2
                            elif(state == 2):
                                vip = token
                                state = 3
                            elif(state == 3):
                                if(token == '}'):
                                    state = 4
                            else:
                                pass
        return vip

    # eligible when keepalived config comprises the ip
    def is_reliable_eligible(self, ip):
        retVal = False
        if ( os.path.exists(self.keepalived_conf) ):
            with open(self.keepalived_conf) as f:
                for line in f:
                    if ip in line:
                        retVal = True
                        break
        return retVal
    
    def is_valid_ip_address(self,address):
        retVal = False
        try:
            list = address.split('.')
            for item in list:
                int(item)
            retVal = True
        except Exception as e:
            pass
        return retVal
    
    # master when current node keepalived answers for head node ip
    # backup when current node keepalived does not answer for head ip, but is capable in config
    # unspecified otherwise
    def get_reliable_state(self):
        label = 'get_reliable_state'
        result = 'unspecified'
        try:
            ducc_head = self.ducc_properties.get('ducc.head')
            if(self.is_valid_ip_address(ducc_head)):
                head_ip = ducc_head
            else:
                head_ip = self.get_ip_address(ducc_head)
            # Check if "reliable" ... i.e. ducc.head is the virtual ip in the keepalived conf file
            # If so check if this node is connected to the virtual ip   
            if(self.is_reliable_eligible(head_ip)):
                text = 'cmd: ', '/sbin/ip', 'addr', 'list'
                debug(label, text)
                p = subprocess.Popen(['/sbin/ip', 'addr', 'list'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output, err = p.communicate()
                text = "output: "+output
                debug(label, text)
                if(head_ip in output):
                    result = 'master'
                else:
                    result = 'backup'
        except Exception as e:
            print e
        return result
    
    def is_reliable_backup(self):
        return self.get_reliable_state() == 'backup'
    
    def db_normalize_component(self,component):
        com = component
        if(component != None):
            if('agent'.startswith(component.lower())):
                com = 'Ag'
            elif('orchestrator'.startswith(component.lower())):
                com = 'Or'
            elif('pm'.startswith(component.lower())):
                com = 'Pm'
            elif('rm'.startswith(component.lower())):
                com = 'Rm'
            elif('sm'.startswith(component.lower())):
                com = 'Sm'
            elif('ws'.startswith(component.lower())):
                com = 'Ws'
            elif('broker'.startswith(component.lower())):
                com = 'Br'
            elif('db'.startswith(component.lower())):
                com = 'Db'
            elif('database'.startswith(component.lower())):
                com = 'Db'
        #print 'db_normalize_component', component, '-->', com
        return com
    
    def db_acct_start(self,node,com):
        label = 'db_acct_start'
        component = self.db_normalize_component(com)
        CMD = [self.java(), '-DDUCC_HOME='+self.DUCC_HOME, 'org.apache.uima.ducc.database.lifetime.DbDaemonLifetimeUI', '--start', node, component]
        text = ' '.join(CMD)
        debug(label,text)
        p = subprocess.Popen(CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        text = out
        debug(label,text)
        
    def db_acct_stop(self,node=None,com=None):
        label = 'db_acct_stop'
        component = self.db_normalize_component(com)
        if((node == None) and (component == None)):
            CMD = [self.java(), '-DDUCC_HOME='+self.DUCC_HOME, 'org.apache.uima.ducc.database.lifetime.DbDaemonLifetimeUI', '--stop']
        elif(component == None):
            CMD = [self.java(), '-DDUCC_HOME='+self.DUCC_HOME, 'org.apache.uima.ducc.database.lifetime.DbDaemonLifetimeUI', '--stop', node]
        else:
            CMD = [self.java(), '-DDUCC_HOME='+self.DUCC_HOME, 'org.apache.uima.ducc.database.lifetime.DbDaemonLifetimeUI', '--stop', node, component]
        text = ' '.join(CMD)
        debug(label,text)
        p = subprocess.Popen(CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        text = out
        debug(label,text)
    
    def _db_get_tuple(self,line):
        retVal = None
        try:
            if(len(line.split()) == 1):
                lhs = line.split('=')[0]
                rhs = line.split('=')[1]
                host = lhs.split('.')[0]
                daemon = lhs.split('.')[1]
                state = rhs
                tuple = [ host, daemon, state ]
                retVal = tuple
        except:
            pass
        return retVal
    
    def db_acct_query(self):
        list = []
        label = 'db_acct_query'
        CMD = [self.java(), '-DDUCC_HOME='+self.DUCC_HOME, 'org.apache.uima.ducc.database.lifetime.DbDaemonLifetimeUI', '--query']
        text = ' '.join(CMD)
        debug(label,text)
        p = subprocess.Popen(CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        text = out
        debug(label,text)
        lines = out.split('\n')
        for line in lines:
            tuple = self._db_get_tuple(line)
            if(tuple != None):
                list.append(tuple)
        return list
        
    def __init__(self, merge=False):
        global use_threading
        DuccBase.__init__(self, merge)

        self.db_disabled = '--disabled--'
        self.db_password = None
        self.duccling = None
        self.broker_url = 'tcp://localhost:61616'
        self.broker_protocol = 'tcp'
        self.broker_host = 'localhost'
        self.broker_port = '61616'
        self.default_components = ['rm', 'pm', 'sm', 'or', 'ws', 'db', 'broker']
        self.local_components = ['rm', 'pm', 'sm', 'or', 'ws', 'broker']
        self.default_nodefiles = [self.DUCC_HOME + '/resources/ducc.nodes']

        if ( self.localhost == self.ducc_properties.get("ducc.head")):
            self.is_ducc_head = True

        os.environ['DUCC_NODENAME'] = self.localhost    # to match java code's implicit property so script and java match

        dbhost = self.get_db_host()
        if ( dbhost == None ):
            dbhost = self.ducc_properties.get('ducc.head')
        if ( dbhost == None ):
            dbhost = 'localhost'


        manage_database = self.ducc_properties.get('ducc.database.automanage')
        self.automanage_database = False
        if (manage_database in ('t', 'true', 'T', 'True')) :
            self.automanage_database = True     

        if(manage_database):
            dir_db_state = self.DUCC_HOME + '/state/database/'+dbhost
            self.makedirs(dir_db_state)
            self.db_pidfile = dir_db_state+ '/cassandra.pid'
            dir_db_logs = self.DUCC_HOME + '/logs'
            self.makedirs(dir_db_logs)
            self.db_logfile = dir_db_logs + '/' + dbhost + '.cassandra.console'
        
        self.pid_file_agents  = self.DUCC_HOME + '/state/agents/ducc.pids'
        self.pid_file_daemons  = self.DUCC_HOME + '/state/daemons/'+self.get_node_name()+'/ducc.pids'
        self.set_classpath()
        self.os_pagesize = self.get_os_pagesize()
        self.update_properties()

        self.db_configure()
        
        manage_broker = self.ducc_properties.get('ducc.broker.automanage')
        self.automanage_broker = False
        if (manage_broker in ('t', 'true', 'T', 'True')) :
            self.automanage_broker = True                    

        py_version = platform.python_version().split('.')
        if ( int(py_version[0]) > 2 ):
            print "Warning, only Python Version 2 is supported."
        if ( int(py_version[1]) < 4 ):
            print "Python must be at least at version 2.4."
            sys.exit(1)
        if ( int(py_version[1]) < 6 ):
            use_threading = False


if __name__ == "__main__":
    util = DuccUtil()

