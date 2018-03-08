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

import subprocess

from ducc_util import DuccUtil

class Master(DuccUtil):

    # purpose:    determine reliable ducc status
    # input:      none
    # output:     one of { unspecified, master, backup }
    # operation:  look in ducc.properties for relevant keywords
    #             and employ linux commands to determine if system
    #             has matching configured virtual ip address

    def main(self):
    	result = 'unspecified'
    	try:
	    	device = self.ducc_properties.get('ducc.virtual.ip.device')
	    	address = self.ducc_properties.get('ducc.virtual.ip.address')
	    	#print "device: "+device, "address: "+address
	    	if(device == None):
	    		pass
	    	elif(address == None):
	    		pass
	    	elif(address == '0.0.0.0'):
	    		pass
	    	else:
	    		#print 'cmd: ', '/sbin/ip', 'addr', 'list', device
	    		p = subprocess.Popen(['/sbin/ip', 'addr', 'list', device], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        		output, err = p.communicate()
        		#print "output: "+output
        		if(address in output):
        			result = 'master'
        		else:
        			result = 'backup'
    	except Exception as e:
    		print e
    	print result
        
if __name__ == '__main__':
    instance = Master()
    instance.main()