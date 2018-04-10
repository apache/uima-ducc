#! /usr/bin/env python
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
import subprocess

from ducc_util import DuccUtil

class DuccHeadMode(DuccUtil):

    # purpose:    determine reliable ducc status
    # input:      none
    # output:     one of { unspecified, master, backup }
    # operation:  look in ducc.properties for relevant keywords
    #             and employ linux commands to determine if system
    #             has matching configured virtual ip address

    debug_flag = False
    
    def debug(self,text):
        if(self.debug_flag):
            print(text)
    
    keepalivd_conf = '/etc/keepalived/keepalived.conf'
    
    # eligible when keepalived config comprises the ip
    def is_reliable_eligible(self, ip):
        retVal = False
        if ( os.path.exists(self.keepalivd_conf) ):
            with open(self.keepalivd_conf) as f:
                for line in f:
                    if ip in line:
                        retVal = True
                        break
        return retVal
    
    # master when current node keepalived answers for head node ip
    # backup when current node keepalived does not answer for head ip, but is capable in config
    # unspecified otherwise
    def main(self):
    	result = 'unspecified'
    	try:
            ducc_head = self.ducc_properties.get('ducc.head')
            head_ip = self.get_ip_address(ducc_head)
            if(self.is_reliable_eligible(head_ip)):
    	    	text = 'cmd: ', '/sbin/ip', 'addr', 'list'
                self.debug(text)
    	    	p = subprocess.Popen(['/sbin/ip', 'addr', 'list'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            	output, err = p.communicate()
            	text = "output: "+output
                self.debug(text)
            	if(head_ip in output):
            		result = 'master'
            	else:
            		result = 'backup'
    	except Exception as e:
    		print e
    	print result
        
if __name__ == '__main__':
    instance = DuccHeadMode()
    instance.main()