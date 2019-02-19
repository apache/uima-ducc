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
import os
import subprocess

from ducc_util import DuccUtil

# command to delete from the autostart database table the specified host & daemon

class AutostartDelete(DuccUtil):

	valid_names = [ 'ag', 'br', 'or', 'pm', 'rm', 'sm', 'ws', 'db' ]
	jclass = 'org.apache.uima.ducc.database.lifetime.DbDaemonLifetimeUI'
	
	description = 'Delete an entry from the autostart database table.'
	
	def get_args(self):
		parser = argparse.ArgumentParser(description=self.description)
		parser.add_argument('--host', action='store', required=True, help='the DUCC daemon host')
		parser.add_argument('--name', action='store', required=True, choices=self.valid_names, help='the DUCC daemon name')
		self.args = parser.parse_args()
	
	def find(self):	
		retVal = False
		option = '--query'
		cmd = [self.jvm, '-DDUCC_HOME='+self.DUCC_HOME, self.jclass, option]
		p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		out, err = p.communicate()
		lines = out.split('\n')
		for line in lines:
			tokens = line.split('.')
			if(len(tokens) == 2):
				host = tokens[0]
				name = tokens[1].split('=')[0]
				if(host == self.args.host):
					if(name == self.args.name):
						retVal = True
		return retVal
	
	def delete(self):	
		option = '--delete'
		cmd = [self.jvm, '-DDUCC_HOME='+self.DUCC_HOME, self.jclass, option, self.args.host, self.args.name]
		p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		out, err = p.communicate()
	
	def main(self, argv):	
		self.get_args()
		if(self.find()):
			self.delete()
			if(self.find()):
				print 'not deleted'
			else:
				print 'deleted'
		else:
			print 'not found'
		
if __name__ == "__main__":

	instance = AutostartDelete()
	instance.main(sys.argv[1:])
