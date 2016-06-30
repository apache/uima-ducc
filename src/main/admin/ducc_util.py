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

import db_util as dbu

global use_threading
use_threading = True

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

    def update_properties(self):

        if ( self.ducc_properties == None ):
            DuccBase.read_properties(self)

        self.duccling          = self.ducc_properties.get('ducc.agent.launcher.ducc_spawn_path')

        # self.broker_url     = self.ducc_properties.get('ducc.broker.url')
        self.broker_protocol   = self.ducc_properties.get('ducc.broker.protocol')
        self.broker_host       = self.ducc_properties.get('ducc.broker.hostname')
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


    def db_configure(self):
        dbhost = self.ducc_properties.get('ducc.database.host')
        if ( dbhost == self.db_disabled ):
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

    # does the database process exist?  
    def db_process_alive(self):
        pidfile = self.DUCC_HOME + '/state/cassandra.pid'

        if ( not os.path.exists(pidfile) ):
            return False

        f = open(self.DUCC_HOME + '/state/cassandra.pid')
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

    # contact the database and see how useful it seems to be
    def db_alive(self, retry=10):
        if ( self.db_bypass == True ):
            return True

        dbnode = self.ducc_properties.get('ducc.database.host')
        if ( dbnode == None ):
            print 'No database location defined.'
            return False

        pidfile = self.DUCC_HOME + '/state/cassandra.pid'
        if ( not os.path.exists(pidfile) ):
            print 'Database pid file does not exist.  Checking DB connectivity.'

        # get our log4j config into the path to shut up noisy logging
        os.environ['CLASSPATH'] = os.environ['CLASSPATH'] + ':' + self.DUCC_HOME + '/resources'
        
        CMD = [self.java(), 'org.apache.uima.ducc.database.DbAlive', dbnode, 'ducc', self.db_password, str(retry)]

        CMD = ' '.join(CMD)
        rc = os.system(CMD)
        if ( rc == 0 ):
            return True
        else:
            return False

            
    def db_start(self):

        # bypass all of this for the initial delivery
        if ( self.db_bypass == True) :
            print '   (Bypass database start because ducc.database.host =', self.db_disabled + ')'
            return True

        print 'Starting database'
        dbnode = self.ducc_properties.get('ducc.database.host')
        dbu.update_cassandra_config(self.DUCC_HOME, dbnode)

        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
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
                if ( line == 'OK' ):
                    print 'GOT OK from db start'
                    lines.close();

            print 'waiting for database to start'
            if ( self.db_alive() ):
                return True

            attempt = attempt + 1
            print 'Did not connect to database, retrying (', attempt, 'of', max_attempts, ')'

        return False

    def db_stop(self):

        if ( self.db_bypass == True) :
            print '   (Bypass database stop because ducc.database.host =', self.db_disabled + ')'
            return True

        pidfile = self.DUCC_HOME + '/state/cassandra.pid'
        if ( os.path.exists(pidfile) ):
            # for cassandra, just send it a terminate signal.  a pidfile is written on startup
            CMD = ['kill', '-TERM', '`cat ' + pidfile + '`']
            CMD = ' '.join(CMD)
            os.system(CMD)

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

        broker_host = self.ducc_properties.get('ducc.broker.hostname')
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
        cmd = ' '.join(cmd)
        # print '**** nohup', cmd, '****'
        devnw = open(os.devnull, 'w')
        devnr = open(os.devnull, 'r')
        ducc = subprocess.Popen(cmd, shell=True, stdin=devnr, stdout=devnw, stderr=devnw)
        devnr.close()
        devnw.close()
        if ( showpid ) :
            print 'PID', ducc.pid

    # like popen, only it spawns via ssh
    def ssh(self, host, do_wait, *CMD):

        cmd = ' '.join(CMD)
        if ( host == self.localhost ):
            if (cmd[0] == "" and cmd[-1] == ""):
                cmd = cmd[1:len(cmd)-2]
                return self.popen(cmd)
        if ( do_wait ):
            return self.popen('ssh -q -o BatchMode=yes -o ConnectTimeout=10', host, cmd)
        else:
            return self.spawn('ssh -q -o BatchMode=yes -o ConnectTimeout=10', host, cmd)


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
        bypass = (user != 'ducc')
        
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

    # Exit if this is not the head node.  Ignore the domain as uname sometimes drops it.
    def verify_head(self):
        head = self.ducc_properties.get("ducc.head").split('.')[0]
        local = self.localhost.split('.')[0]
        if local != head:
            print ">>> ERROR - this script must be run from the head node"
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
            if ( os.environ['LOGNAME'] == 'ducc' ):
                print 'ducc_ling module', dl, ': setuid bit is not set. Processes will run as user ducc'
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
                grpinfo = grp.getgrnam('ducc')
                duccgid = grpinfo.gr_gid

                if ( (dl_stat.st_uid != 0) or (dl_stat.st_gid != duccgid) ):
                    print 'ducc_ling module', dl, ': Invalid ownership. Should be root.ducc'
                else:
                    own_safe = True
            except:
                print 'ducc_ling group "ducc" cannot be found.'

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
        
        if ( user == 'ducc' ):
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

            if ( not ('java' in procname) ):
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
    # Given the name of a file containing ducc nodes, a ducc user (usually 'ducc' unless you're running
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
    #
    def read_nodefile(self, nodefile, ret):
        #print 'READ_NODEFILE:', nodefile, ret
        n_nodes = 0
        if ( os.path.exists(nodefile) ):
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

    def disable_threading(self):
        global use_threading
        use_threading = False

    def installed(self):
        head = self.ducc_properties.get('ducc.head')
        if ( head == '<head-node>' ):
            return False
        return True

    def __init__(self, merge=False):
        global use_threading
        DuccBase.__init__(self, merge)

        self.db_disabled = '--disabled--'
        self.duccling = None
        self.broker_url = 'tcp://localhost:61616'
        self.broker_protocol = 'tcp'
        self.broker_host = 'localhost'
        self.broker_port = '61616'
        self.default_components = ['rm', 'pm', 'sm', 'or', 'ws', 'db', 'broker']
        self.default_nodefiles = [self.DUCC_HOME + '/resources/ducc.nodes']

        if ( self.localhost == self.ducc_properties.get("ducc.head")):
            self.is_ducc_head = True

        os.environ['DUCC_NODENAME'] = self.localhost    # to match java code's implicit propery so script and java match

        self.pid_file  = self.DUCC_HOME + '/state/ducc.pids'
        self.set_classpath()
        self.os_pagesize = self.get_os_pagesize()
        self.update_properties()

        self.db_configure()
        

        manage_broker = self.ducc_properties.get('ducc.broker.automanage')
        self.automanage = False
        if (manage_broker in ('t', 'true', 'T', 'True')) :
            self.automanage = True                    

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

