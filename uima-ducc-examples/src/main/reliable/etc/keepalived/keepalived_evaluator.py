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
import syslog

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

import os
import subprocess
import traceback

ducc_runtime = '/home/ducc/ducc_runtime'
ducc_status = os.path.join(ducc_runtime,'bin/ducc_status')

state_dir = fpath = __file__.split('/')[0]

class Keepalived_Evaluator():
    
    flag_error = True
    flag_debug = False
    flag_info = True
    
    def __init__(self):
        pass
    
    def _cn(self):
        return self.__class__.__name__

    def _mn(self):
        return traceback.extract_stack(None,2)[0][2]
    
    def error(self,mn,text):
        if(self.flag_error):
            print mn+' '+text
    
    def debug(self,mn,text):
        if(self.flag_debug):
            print mn+' '+text
            
    def info(self,mn,text):
        if(self.flag_info):
            print mn+' '+text
    
    def to_syslog(self,mn,text):
        cn = self._cn()
        syslog.syslog(cn+'.'+mn+': '+text)
    
    def do_cmd(self,cmd):
        mn = self._mn()
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        text = 'out:'+str(out)
        self.debug(mn,text)
        text = 'err:'+str(err)
        self.debug(mn,text)
        return out,err
    
    def get_ducc_status(self):
        mn = self._mn()
        cmd = [ducc_status,'--target','localhost','-e']
        out, err = self.do_cmd(cmd)
        if('down=0' in out):
            status = 0
        else:
            status = 1
            text = 'out: '+str(out)
            self.to_syslog(mn,text)
            text = 'rc: '+str(status)
            self.to_syslog(mn,text)
        text = str(status)
        self.debug(mn,text)
        return status
    
    def main(self, argv):
        mn = self._mn()
        rc = 0
        try:
            rc = self.get_ducc_status()
        except Exception as e:
            lines = traceback.format_exc().splitlines()
            for line in lines:
                text = line
                self.error(mn,text)
                self.to_syslog(mn,text)
            rc = 1
            text = 'rc: '+str(rc)
            self.to_syslog(mn,text)
        text = 'rc='+str(rc)
        self.info(mn,text)
        sys.exit(rc)

if __name__ == "__main__":
    instance = Keepalived_Evaluator()
    instance.main(sys.argv[1:])
    
