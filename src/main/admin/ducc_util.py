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
import zipfile
import resource
import time
from  stat import *

class DuccPropertiesException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)

class DuccProperties:
    def __init__(self):
        self.props = {}

    #
    # Expand ${} values from env or from this properties file itself
    #
    def do_subst(self, str):
    
        key = None
        p = re.compile("\\$\\{[a-zA-Z0-9_]+\\}")
        m = p.match(str)
        
        if ( m != None ):
            key = m.group()[2:-1]
            #print str, m, m.group(), key
            val = os.environ[key]
            response = string.replace(str, m.group() , val)
        else:
            response = str
            
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
            val = self.do_subst(val)
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
            return self.props[key]
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
    
class DuccUtil:


    def read_properties(self):

        self.ducc_properties = DuccProperties()
        self.ducc_properties.load(self.propsfile)

        self.duccling       = self.ducc_properties.get('ducc.agent.launcher.ducc_spawn_path')
        self.webserver_node = self.ducc_properties.get('ducc.ws.node')
        self.jvm            = self.ducc_properties.get('ducc.jvm')
        # self.broker_url     = self.ducc_properties.get('ducc.broker.url')
        self.broker_protocol   = self.ducc_properties.get('ducc.broker.protocol')
        self.broker_host       = self.ducc_properties.get('ducc.broker.hostname')
        self.broker_port       = self.ducc_properties.get('ducc.broker.port')
        self.broker_jmx_port   = self.ducc_properties.get('ducc.broker.jmx.port')
        self.broker_decoration = self.ducc_properties.get('ducc.broker.url.decoration')
        self.broker_url        = self.broker_protocol + '://' + self.broker_host + ':' + self.broker_port
        self.agent_jvm_args         = self.ducc_properties.get('ducc.agent.jvm.args')
        self.ws_jvm_args            = self.ducc_properties.get('ducc.ws.jvm.args')
        self.pm_jvm_args            = self.ducc_properties.get('ducc.pm.jvm.args')
        self.rm_jvm_args            = self.ducc_properties.get('ducc.rm.jvm.args')
        self.sm_jvm_args            = self.ducc_properties.get('ducc.sm.jvm.args')
        self.or_jvm_args            = self.ducc_properties.get('ducc.orchestrator.jvm.args')

        if ( self.broker_decoration == '' ):
            self.broker_decoration = None

        if ( self.broker_decoration != None ):
            self.broker_url = self.broker_url + '?' + self.broker_decoration
        
        if ( self.webserver_node == None ):
            self.webserver_node = self.localhost

    def java(self):
        if ( self.jvm == None ):
            return 'java'
        else:
            return self.jvm
        
    def is_amq_active(self):
        lines = self.popen('ssh', self.broker_host, 'netstat -an')
        #
        # look for lines like this with the configured port in the 4th token, and
        # ending with LISTEN:
        #
        # tcp        0      0 :::61616                :::*                    LISTEN      
        for line in lines:
            toks = line.split()
            if ( toks[-1] == 'LISTEN' ):
                port = toks[3]
                if (port.endswith(self.broker_port)):
                    return True
        return False        

    def version(self):
        lines = self.popen(self.jvm, ' org.apache.uima.ducc.utils.Version')
        line = lines.readline().strip()
        return "DUCC Version", line
        
    def nohup_new(self, cmd, showpid=True):
        nfds = resource.getrlimit(resource.RLIMIT_NOFILE)[1]      # returns softlimit, hardlimit

        # print 'NOHUP', cmd
        print 'NOHUP', ' '.join(cmd)
        print 'NOHUP', os.environ['IP']
        print 'NOHUP', os.environ['NodeName']
        #print 'NOHUP', os.environ['CLASSPATH']
        try:
            pid = os.fork()
        except OSError, e:
            raise Exception, "%s [%d]" % (e.strerror, e.errno)

        if ( pid != 0 ):
            return            # the parent
        else:
            os.setsid()

            try:
                pid = os.fork()
            except OSError, e:
                raise Exception, "%s [%d]" % (e.strerror, e.errno)

            if ( pid != 0 ):
                if ( showpid ):
                    os.write(1, 'PID ' + str(pid) + '\n')
                return
            
            print 'NOHUP flushing'
            sys.stdout.flush()
            nfds = resource.getrlimit(resource.RLIMIT_NOFILE)[1]      # returns softlimit, hardlimit
            for i in range(3, nfds):
                try:
                    #os.close(i);
                    pass
                except:
                    pass      # wasn't open

            #devnull = os.devnull
            #open(devnull, 'r')  # fd 0 stdin
            #open(devnull, 'w')  #    1 stdout
            #open(devnull, 'w')  #    2 stderr
            os.execvp(cmd[0], cmd)


    def nohup(self, cmd, showpid=True):
        cmd = ' '.join(cmd)
        print '**** nohup', cmd, '****'
        devnw = open(os.devnull, 'w')
        devnr = open(os.devnull, 'r')
        ducc = subprocess.Popen(cmd, shell=True, stdin=devnr, stdout=devnw, stderr=devnw)
        devnr.close()
        devnw.close()
        if ( showpid ) :
            print 'PID', ducc.pid

    # simply spawn-and-forget using Python preferred mechanism
    def spawn(self, *CMD):
        cmd = ' '.join(CMD)
        # print '**** spawn', cmd, '****'
        ducc = subprocess.Popen(cmd, shell=True)
        pid = ducc.pid
        status = os.waitpid(pid, 0)
        return pid

    def popen(self, *CMD):
        cmd = ' '.join(CMD)
        #print 'POPEN:', cmd
        proc = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        return proc.stdout

    # like popen, only it spawns via ssh
    def ssh(self, host, do_wait, *CMD):

        cmd = ' '.join(CMD)
        #print 'ssh -o BatchMode=yes -o ConnectTimeout=10', host, cmd
        if ( do_wait ):
            return self.popen('ssh -o BatchMode=yes -o ConnectTimeout=10', host, cmd)
        else:
            return self.spawn('ssh -o BatchMode=yes -o ConnectTimeout=10', host, cmd)


    def set_classpath(self):
        ducc_home = self.DUCC_HOME
        LIB       = ducc_home + '/lib'
        RESOURCES = ducc_home + '/resources'
    
        CLASSPATH = ''
    
        CLASSPATH = CLASSPATH + ":" + LIB + '/apache-activemq-5.5.0/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/apache-commons-cli-1.2/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/apache-commons-lang-2.6/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/guava-r09/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/apache-log4j-1.2.16/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/uima/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/apache-camel-2.7.1/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/apache-commons-collections-3.2.1/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/joda-time-1.6/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/springframework-3.0.5/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/xmlbeans-2.5.0/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/bluepages/*'

        # orchestrator http needs codecs
        CLASSPATH = CLASSPATH + ":" + LIB + '/http-client/*'

        # explicitly NOT ducc_test.jar
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-ibm.jar'
        CLASSPATH = CLASSPATH + ':' + ducc_home + '/webserver/lib/*'
        CLASSPATH = CLASSPATH + ':' + ducc_home + '/webserver/lib/jsp/*'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-agent.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-cli.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-common.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-jd.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-orchestrator.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-pm.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-rm.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-sm.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-web.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-viz.jar'

        CLASSPATH = CLASSPATH + ':' + RESOURCES
    
        os.environ['CLASSPATH'] = CLASSPATH

    def set_classpath_for_clix(self):
        ducc_home = self.DUCC_HOME
        LIB       = ducc_home + '/lib'
        RESOURCES = ducc_home + '/resources'
    
        CLASSPATH = ''
    
        CLASSPATH = CLASSPATH + ":" + LIB + '/apache-activemq-5.5.0/activemq-all-5.5.0.jar'
        CLASSPATH = CLASSPATH + ":" + LIB + '/apache-commons-cli-1.2/commons-cli-1.2.jar'
        CLASSPATH = CLASSPATH + ":" + LIB + '/apache-camel-2.7.1/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/http-client/*'
        CLASSPATH = CLASSPATH + ":" + LIB + '/springframework-3.0.5/*'

        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-cli.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/ducc-common.jar'

        CLASSPATH = CLASSPATH + ':' + LIB + '/uima/uima-core.jar'
        CLASSPATH = CLASSPATH + ':' + LIB + '/uima/uimaj-as-core.jar'
        
        CLASSPATH = CLASSPATH + ':' + RESOURCES

        os.environ['CLASSPATH'] = CLASSPATH

    def set_classpath_for_submit(self):
        ducc_home = self.DUCC_HOME
        LIB       = ducc_home + '/lib'
        
        CLASSPATH = LIB + '/ducc-submit.jar'
        os.environ['CLASSPATH'] = CLASSPATH

    def verify_duccling(self):
        
        check_permission = True                        # if we're not ducc we don't care about permissions
        user = os.environ['LOGNAME']
        if ( user != 'ducc' ):
            check_permission = False
                    
        if ( check_permission ) :            # only care about ducc_ling setup if we're ducc
            path = os.path.dirname(os.path.abspath(self.duccling))
            dl   = path + '/ducc_ling'

            sstat = os.stat(path)
            mode = sstat.st_mode
            if ( not S_ISDIR(mode) ):
                print 'ducc_ling path', path, ': Not a directory.'
                return False
            
            dirown = mode & (S_IRWXU | S_IRWXG | S_IRWXO)
            #print 'Directory perms', oct(dirown)
            if ( dirown != S_IRWXU ):
                 print 'ducc_ling path', path, ': Invalid directory permissions', oct(dirown), 'should be', oct(S_IRWXU) 
                 return False
             
            sstat = os.stat(dl)
            mode = sstat.st_mode
            expected = (S_IRWXU | S_IRGRP | S_IXGRP)
            pathown = mode & (S_IRWXU | S_IRWXG | S_IRWXO)
            #print 'Duccling perms', oct(pathown)
            if ( pathown != expected ):
                print 'ducc_ling module', dl, ': Invalid permissions', oct(pathown), 'Should be', oct(expected)
                return False
            
            if ( (mode & S_ISUID) != S_ISUID):
                print 'ducc_ling module', dl, ': setuid bit is not set'
                return False
             
            try:
                grpinfo = grp.getgrnam('ducc')
            except:
                print 'ducc_ling group "ducc" cannot be found.'
                return false

            duccgid = grpinfo.gr_gid
            #print 'UID', sstat.st_uid, 'GID', duccgid
            if ( (sstat.st_uid != 0) or (sstat.st_gid != duccgid) ):
                 print 'ducc_ling module', dl, ': Invalid ownership. Should be ducc.ducc'
                 return False
             
        print 'ducc_ling OK'
        return True
                 
    #
    # Input is array lines from ps command looking for ducc processes owned this user.
    # Output is list of dictionaries, where each dictionary describes a ducc process.
    #
    # If no ducc processes are found here the list is empty.
    #
    # The caller executes the 'ps' command and knows the node this is for.
    #
    def find_ducc_process(self, node, user):
    
        answer = []
        if ( user == 'all' ) :
            resp = self.ssh(node, True, "'", 'ps auxw | grep java ', "'")
        else:
            resp = self.ssh(node, True, "'", 'ps auxw | grep ' + user + ' | grep java ', "'")
            
        while True:
            line = resp.readline().strip()            
            if ( line.startswith("Permission denied") ):
                print node, "ALERT: Passwordless SSH is not configured correctly for node", node
                print node, "ALERT: SSH returns '" + line + "'"
                break
            
            if ( line.startswith("Host key verification failed") ):
                    print node, "ALERT: Passwordless SSH is not configured correctly for node", node
                    print node, "ALERT: SSH returns '" + line + "'"
                    break

            if ( line.find("Connection refused") >= 0 ):
                print node, "ALERT: SSH is not not enabled on node", node
                print node, "ALERT: SSH returns '" + line + "'"
                break
            
            if ( line.find("Connection timed") >= 0 ):
                print node, "ALERT: SSH did not respond with timeout of 10 secnds", node
                print node, "ALERT: SSH returns '" + line + "'"
                break
            
            if ( not line ):
                    break

            toks = line.split()
            if ( len(toks) < 11 ):
                continue

            pid = toks[1]
            procname = toks[10]

            if ( (user != 'all') and (toks[0] != user) ):
                continue

            if ( procname.endswith('java') ):
                for tok in toks[10:]:
                    if ( tok.startswith('-Dducc.deploy.components=') ):
                        cmp = tok.split('=')
                        dp = (cmp[1],  pid, toks[0])
                        answer.append(dp)
                        
        return answer

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



    #def read_nodefile(self, nodefile, nodes):
    #
    #    if ( not os.path.exists(nodefile) ):
    #        print 'Nodefile', nodefile, 'does not exist or cannot be read.'
    #        return None
    # 
    #     f = open(nodefile)
    #     for node in f:
    #         node = node.strip()
    #         if ( not node ):
    #             continue
    #         if ( node.startswith('#') ):
    #             continue
    #         nodes.append(node)
    #
    #       return nodes
    
    def remove_orchestrator_lock(self):
        orlock = self.DUCC_HOME + '/state/orchestrator.lock'
        try:
            if ( os.path.exists(orlock) ):
                os.remove(orlock)
            print 'Orchestrator lock removed'
        except:
            print 'Unable to remove orchestrator lock'

    def kill_process(self, node, proc):
        self.ssh(node, False, 'kill', '-KILL', proc[1])
                
    def clean_shutdown(self):
        DUCC_JVM_OPTS = ' -Dducc.deploy.configuration=' + self.DUCC_HOME + "/resources/ducc.properties "
        self.spawn('java', DUCC_JVM_OPTS, 'org.apache.uima.ducc.common.main.DuccAdmin', '--killAll')

    def get_os_pagesize(self):
        lines = self.popen('/usr/bin/getconf', 'PAGESIZE')
        return lines.readline().strip()

    def show_ducc_environment(self):

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
            lines = self.popen(jvm + ' -fullversion')
            for line in lines:
                response.append('ENV: ' + line.strip())
                

        #
        # Get the total memory for the node
        #
        meminfo = DuccProperties()
        meminfo.load('/proc/meminfo')
        mem = meminfo.get('MemTotal')
        if ( mem.endswith('kB') ):
            toks = mem.split(' ')
            mem = str(int(toks[0]) / (1024*1024)) + ' gB'
        response.append('MEM: memory is ' + mem)

        #
        # Get the operating system information
        #
        f = open('/proc/version')
        for line in f:
            response.append('ENV: system is ' + line.strip())
        f.close()

        #
        # Print the version information from the DUCC jars
        #
        for j in [\
                  'ducc-rm.jar',\
                  'ducc-pm.jar', \
                  'ducc-orchestrator.jar', \
                  'ducc-sm.jar', \
                  'ducc-web.jar', \
                  'ducc-cli.jar', \
                  'ducc-agent.jar', \
                  'ducc-common.jar', \
                  'ducc-jd.jar', \
                  'ducc-test.jar', \
                  'ducc-ibm.jar' \
                 ]:

            manifest = DuccProperties()
            manifest.load_from_manifest(self.DUCC_HOME + '/lib/' + j)
            response.append('ENV: %25s %18s %12s %s' % (j + ':', manifest.get('Ducc-Version'), 'compiled at', manifest.get('Ducc-Build-Date')))

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
                    ret = self.read_nodefile(newfile, ret)
                    continue
                nodes.append(node)
            ret[nodefile] = nodes
        else:
            print 'Cannot read nodefile', nodefile
            ret[nodefile] = None

        #print 'RETURN', nodefile, ret
        return ret

    def __init__(self):

        if ( os.environ.has_key('DUCC_HOME') ):
            self.DUCC_HOME = os.environ['DUCC_HOME']
        else:
            me = os.path.abspath(sys.argv[0])    
            ndx = me.rindex('/')
            ndx = me.rindex('/', 0, ndx)
            self.DUCC_HOME = me[:ndx]          # split from 0 to ndx
            os.environ['DUCC_HOME'] = self.DUCC_HOME

        self.jvm = None
        self.webserver_node = 'localhost'
        self.duccling = None
        self.broker_url = 'tcp://localhost:61616'
        self.broker_protocol = 'tcp'
        self.broker_host = 'localhost'
        self.broker_port = '61616'
        self.default_components = ['rm', 'pm', 'sm', 'or', 'ws', 'viz']
        self.default_nodefiles = [self.DUCC_HOME + '/resources/ducc.nodes']
        self.propsfile = self.DUCC_HOME + '/resources/ducc.properties'
        self.localhost = os.uname()[1]                
        self.pid_file  = self.DUCC_HOME + '/state/ducc.pids'
        self.set_classpath()
        self.read_properties()       
        self.os_pagesize = self.get_os_pagesize()

if __name__ == "__main__":
    util = DuccUtil()

