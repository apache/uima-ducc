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

import sys

version_min = [2, 7]
version_info = sys.version_info
version_error = False
if(version_info[0] < version_min[0]):
    version_error = True
elif(version_info[0] == version_min[0]):
    if(version_info[1] < version_min[1]):
        version_error = True
if(version_error):
    print('Python minimum requirement is version '+str(version_min[0])+'.'+str(version_min[1]))
    sys.exit(1)

import argparse
import datetime
import logging
import os
import shlex
import socket
import subprocess
import time
import traceback

from ducc_util import DuccUtil

# produce a time stamp
def get_timestamp():
    tod = time.time()
    timestamp = datetime.datetime.fromtimestamp(tod).strftime('%Y-%m-%d %H:%M:%S')      
    return timestamp

# embedded class to log to file
# default level is info level, 
# controlled by environment variable LOGLEVEL = { info, debug, trace }
class Logger():
    
    flag_info = True
    flag_debug = False
    flag_trace = False
    
    flag_error = True
    flag_warn = True
    
    def __init__(self,filename,level='info'):
        self.filename = filename
        if(level == 'debug'):
            self.flag_debug = True
        elif(level == 'trace'):
            self.flag_debug = True
            self.flag_trace = True
    
    # write to file
    def output(self,type,mn,text):
        message = get_timestamp()+' '+type+' '+mn+' '+text
        with open(self.filename, 'a') as logfile:
            logfile.write(message+'\n')
    
    # record info message
    def info(self,mn,text):
        if(self.flag_info):
            self.output('I',mn,text)
    
    # record debug message
    def debug(self,mn,text):
        if(self.flag_debug):
            self.output('D',mn,text)
    
    # record trace message
    def trace(self,mn,text):
        if(self.flag_trace):
            self.output('T',mn,text)
          
    # record error message        
    def error(self,mn,text):
        if(self.flag_error):
            self.output('E',mn,text)  
            
    # record warn message
    def warn(self,mn,text):
        if(self.flag_warn):
            self.output('W',mn,text) 

# class to automagically start DUCC daemons                        
class AutoStart(DuccUtil):

    components = [ 'agent', 'broker', 'orchestrator', 'pm', 'rm', 'sm', 'ws' ]
    map = { 'ag':'agent', 
            'br':'broker',
            'or':'or',
            'pm':'pm',
            'rm':'rm',
            'sm':'sm',
            'ws':'ws',
    }
    
    def __init__(self):
        DuccUtil.__init__(self)
        self.ssh_enabled = False
        
    # return file name
    def _fn(self):
        fpath = __file__.split('/')
        flen = len(fpath)
        return fpath[flen-1]

    # return class name
    def _cn(self):
        return self.__class__.__name__
    
    # return method name
    def _mn(self):
        return traceback.extract_stack(None,2)[0][2]
    
    description = 'Start daemon(s) on the present node when listed in the autostart database table but not already running.'
    
    def get_args(self):
        parser = argparse.ArgumentParser(description=self.description)
        self.args = parser.parse_args()

    # setup logging to file for autostart script
    def setup_logging(self,NAME):
        LOGDIR = self.DUCC_HOME+'/logs/'+NAME
        self.makedirs(LOGDIR)
        self.LOCAL_HOST = socket.getfqdn()
        FN = self.LOCAL_HOST.split('.')[0]+'.'+NAME+'.log'
        LOGFILE = LOGDIR+'/'+FN
        LOGLEVEL = os.environ.get('LOGLEVEL','info')
        self.logger = Logger(LOGFILE,LOGLEVEL)
        
    # check if host names with domain match
    def is_host_match_with_domain(self,h1,h2):
        retVal = False
        if(h1 == h2):
            retVal = True
        text = str(h1)+' '+str(h2)+' '+str(retVal)
        self.logger.debug(self._mn(),text)
        return retVal
    
    # check if host names without domain match
    def is_host_match_without_domain(self,h1,h2):
        retVal = False
        h10 = h1.split('.')[0]
        h20 = h2.split('.')[0]
        if(h10 == h20):
            retVal = True
        text = str(h10)+' '+str(h20)+' '+str(retVal)
        self.logger.debug(self._mn(),text)
        return retVal
    
    #check if host names match with/without domain
    def is_host_match(self,h1,h2):
        retVal = False
        if(h1 != None):
            if(h2 != None):
                if(self.is_host_match_with_domain(h1,h2)):
                    retVal = True
                elif(self.is_host_match_without_domain(h1,h2)):
                    retVal = True
        text = str(h1)+' '+str(h2)+' '+str(retVal)
        self.logger.debug(self._mn(),text)
        return retVal    

    def parse_line(self,line):
        retVal = '', '', ''
        try:
            if(line != None):
                text = line
                self.logger.debug(self._mn(),text)
                tokens = line.split()
                if(len(tokens) == 1):
                    host = line.split('.')[0]
                    remainder = line.split('.')[1]
                    name = remainder.split('=')[0]
                    state = remainder.split('=')[1]
                    retVal = host, name, state
        except:
            pass
        return retVal
    
    # get daemons started/all in DB for host
    def get_daemons_host(self):
        db = []
        started = []
        jclass = 'org.apache.uima.ducc.database.lifetime.DbDaemonLifetimeUI'   
        option = '--query'
        cmd = [self.jvm, '-DDUCC_HOME='+self.DUCC_HOME, jclass, option]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        lines = out.split('\n')
        for line in lines:
            host, daemon, state = self.parse_line(line)
            if(self.is_host_match(self.LOCAL_HOST, host)):
                db.append(daemon)
                if(state == 'Start'):
                    started.append(daemon)
                    text = 'add'+' '+host+' '+daemon
                    self.logger.debug(self._mn(),text)
                else:
                   text = 'skip'+' '+host+' '+daemon
                   self.logger.debug(self._mn(),text) 
            else:
                text = 'skip'+' '+host+' '+daemon
                self.logger.debug(self._mn(),text)
        text = 'started'+' '+str(started)+' '+'db'+' '+str(db)
        self.logger.debug(self._mn(),text)
        return db, started

    def normalize_component(self,component):
        daemon = component[:2]
        return daemon
        
    # get daemons running (from system)
    def get_components_running(self):
        daemons = []
        result = self.find_ducc_process(self.LOCAL_HOST)
        find_status = result[0]
        text = 'find_status:'+str(find_status)
        self.logger.debug(self._mn(),text)
        tuples = result[1]
        text = 'tuples:'+str(tuples)
        self.logger.debug(self._mn(),text)
        for tuple in tuples:
            component = tuple[0]
            pid = tuple[1]
            user = tuple[2]
            if(user == self.ducc_uid):
                if(component in self.components):
                    text = 'keep:'+str(tuple)
                    self.logger.debug(self._mn(),text)
                    daemon = self.normalize_component(component)
                    daemons.append(daemon)
                else:
                    text = 'skip:'+str(tuple)
                    self.logger.debug(self._mn(),text)
            else:
                text = 'skip:'+str(tuple)+' '+str(self.ducc_uid)
                self.logger.debug(self._mn(),text)
        text = 'daemons:'+str(daemons)
        self.logger.debug(self._mn(),text)
        return daemons
    
    def start(self,daemon):
        component = self.map[daemon]
        text = str(component)
        self.logger.warn(self._mn(),text)
        if(daemon == 'ag'):
            python_script = os.path.join(self.DUCC_HOME,'admin','ducc.py')
            cmd = [ python_script, '-c', component, '-b', '-d', str(time.time()), '--nodup' ]
        else:
            python_script = os.path.join(self.DUCC_HOME,'admin','start_ducc',)
            cmd = [ python_script, '-c', component ]
        text = str(cmd)
        self.logger.info(self._mn(),text)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        text = str(out)
        self.logger.info(self._mn(),text)
    
    def insert(self):
        python_script = os.path.join(self.DUCC_HOME,'admin','db_autostart_insert.py')
        node = self.get_node_name()
        component = 'ag'
        cmd = [ python_script, '--host', node, '--name', component]
        text = str(cmd)
        self.logger.info(self._mn(),text)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        text = str(out)
        self.logger.info(self._mn(),text)
        
    # autostart: start head or agent daemons, as required
    def main(self, argv):
        NAME = 'autostart'
        self.setup_logging(NAME)
        self.get_args()
        try:
            daemons_db, daemons_started = self.get_daemons_host()
            text = 'daemons_db '+str(len(daemons_db))
            self.logger.debug(self._mn(),text)
            text = 'daemons_started '+str(len(daemons_started))
            self.logger.debug(self._mn(),text)
            # if agent node is not in db, then insert it!
            if(len(daemons_db) == 0):
                if(self.is_head_node()):
                    pass
                else:
                    self.insert()
                    daemons_db, daemons_started = self.get_daemons_host()
            components_running = self.get_components_running()
            text = 'components_running '+str(components_running)
            self.logger.debug(self._mn(),text)
            for daemon in daemons_started:
                component = self.normalize_component(daemon)
                text = 'component '+str(component)
                self.logger.debug(self._mn(),text)
                if(component in components_running):
                    pass
                else:
                    text = 'start daemon '+str(daemon)
                    self.logger.debug(self._mn(),text)
                    self.start(daemon)
        except Exception,e:
            lines = traceback.format_exc().splitlines()
            for line in lines:
                text = line
                self.logger.debug(self._mn(),text)
        
if __name__ == '__main__':
    instance = AutoStart()
    instance.main(sys.argv[1:])
    