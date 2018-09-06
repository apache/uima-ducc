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
#	 http://www.apache.org/licenses/LICENSE-2.0
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
import os
import time
import traceback

from ducc_util import DuccUtil

# print message
def output(msg):
    print msg
    
# produce a time stamp
def get_timestamp():
    tod = time.time()
    timestamp = datetime.datetime.fromtimestamp(tod).strftime('%Y-%m-%d %H:%M:%S')      
    return timestamp

_flag_debug = False

# record debug message
def debug(mn,text):
    if(_flag_debug):
        type ='D'
        msg = get_timestamp()+' '+type+' '+mn+' '+text
        output(msg)

# command to check db access authorization

class AccessCheck(DuccUtil):
	
	# return method name
	def _mn(self):
		return traceback.extract_stack(None,2)[0][2]
	
	description = 'Determine if LOOKER can view OWNER database data through examination of db.access file in security directory, typically ~/.ducc.'\
				+ '  Return 1 if authorized, 0 otherwise.'\
				+ '  Rules: '\
				+ '  1. Authorized if OWNER == LOOKER'\
				+ '  or '\
				+ '  2. Authorized if OWNER db.access file is readable by all'\
				+ '  or '\
				+ '  3. Authorized if LOOKER groups contains the OWNER db.access file group'\
				+ ''
	
	def get_args(self):
		global _flag_debug
		parser = argparse.ArgumentParser(description=self.description)
		parser.add_argument('--owner', '-o', action='store', required=True, help='the user who owns the data')
		parser.add_argument('--looker', '-l', action='store', required=True, help='the user who views the data')
		parser.add_argument('--debug', '-d', action='store_true', help='display debugging messages')
		self.args = parser.parse_args()
		_flag_debug = self.args.debug
	
	# get property from ducc.properties file
	def get_ducc_property(self, key):
		value = self.ducc_properties.get(key)
		text = 'key:'+key+' '+'value:'+value
		debug(self._mn(),text)
	
	# get DUCC security home directory, by default user's $HOME
	def get_security_home(self):
		key = 'ducc.security.home'
		value = self.get_ducc_property(key)
		if(value == None):
			home_folder = os.getenv('HOME')
			value = home_folder.rsplit('/',1)[0]
		self.ducc_security_home = value
		text = value
		debug(self._mn(),text)
	
	# Access: (0644/-rw-r--r--)  Uid: ( 2077/watson)   Gid: ( 2001/limited)
	def get_permissions(self,line):
		normalized_line = line
		normalized_line = normalized_line.replace('(',' ')
		normalized_line = normalized_line.replace(')',' ')
		tokens = normalized_line.split()
		permissions = tokens[1]
		permissions = permissions.replace('/',' ')
		permissions = permissions.strip()
		permissions = permissions.split()[1]
        # -rw-r--r--
		return permissions
	
	# Access: (0644/-rw-r--r--)  Uid: ( 2077/watson)   Gid: ( 2001/limited)
	def get_group(self,line):
		normalized_line = line
		normalized_line = normalized_line.replace('(',' ')
		normalized_line = normalized_line.replace(')',' ')
		tokens = normalized_line.split()
		group = tokens[5]
		group = group.replace('/',' ')
		group = group.strip()
		group = group.split()[1]
        # limited
		return group
	
	# get db.access file info, comprising file permissions and file group
	def get_owner_access_data(self):
		permissions = '----------'
		group = ''
		ducc_ling = self.duccling
		self.get_security_home()
		security_file = os.path.join(self.ducc_security_home,self.args.owner,'.ducc','db.access')
		text = 'security_file:'+security_file
		debug(self._mn(),text)
		p0 = '-q'
		p1 = '-u'
		p2 = self.args.owner
		p3 = '/usr/bin/stat'
		p4 = security_file
		cmd = ducc_ling+' '+p0+' '+p1+' '+p2+' '+p3+' '+p4
		text = cmd
		debug(self._mn(),text)
		lines = self.popen(ducc_ling,p0,p1,p2,p3,p4)
		for line in lines:
			line = line.strip()
			if(('Access:' in line) and ('Gid:' in line)):
				permissions = self.get_permissions(line)
				group = self.get_group(line)
				text = 'permissions:'+permissions+' '+'group:'+group
				debug(self._mn(),text)
				break;
		return permissions, group
	
	# get looker's groups
	def get_looker_groups(self):
		groups = []
		ducc_ling = self.duccling
		p0 = '-q'
		p1 = '-u'
		p2 = self.args.owner
		p3 = '/usr/bin/groups'
		cmd = ducc_ling+' '+p0+' '+p1+' '+p2+' '+p3
		text = cmd
		debug(self._mn(),text)
		lines = self.popen(ducc_ling,p0,p1,p2,p3)
		for line in lines:
			line = line.strip()
			tokens = line.split()
			groups = tokens
		return groups
	
	# check if looker is authorized to access owner's db info
	def check_authorization(self):
		authorized = 0
		text = 'owner:'+self.args.owner+' '+'looker:'+self.args.looker
		debug(self._mn(),text)
		if(self.args.owner == self.args.looker):
			authorized = 1
			text = 'authorized:'+str(authorized)+' '+'reason: owner == looker'
			debug(self._mn(),text)
		else:
			file_permissions, file_group = self.get_owner_access_data()
			text = 'permissions:'+file_permissions+' '+'group:'+file_group
			debug(self._mn(),text)
			if(file_permissions[7] == 'r'):
				authorized = 1
				text = 'authorized:'+str(authorized)+' '+'reason: owner db.access file readable by all'
				debug(self._mn(),text)
			elif(file_permissions[4] == 'r'):
				looker_groups = self.get_looker_groups()
				text = 'groups:'+str(looker_groups)
				debug(self._mn(),text)
				if(file_group in looker_groups):
					authorized = 1
					text = 'authorized:'+str(authorized)+' '+'reason: looker in group assigned to db.access file'
					debug(self._mn(),text)
		return authorized
		
	def main(self, argv):
		authorized = 0
		self.get_args()
		authorized = self.check_authorization()
		print str(authorized)
		sys.exit(authorized)
		
if __name__ == '__main__':
	instance = AccessCheck()
	instance.main(sys.argv[1:])
