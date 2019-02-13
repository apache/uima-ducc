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

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +
# + duccmon_pwgen.py
# +
# + purpose: generate login pw for SecureFileAuthenticator
# + 
# + input: none (DUCC_HOME implied from location of this script)
# + 
# + output: none (file <security-home>/.ducc/.login.pw updated)
# + 
# + exit code: 0
# +
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


import getpass
import os
import random
import sys

from ducc_util import DuccUtil
from properties import Properties
from properties import Property

class PwGen(DuccUtil):

	# generated password minimum length
	pw_len_min = 8
	
	# generated password maximum length
	pw_len_max = 16
	
	# generated password character set
	pw_alphabet = ',.?-+=0123456789abcdefghijklmnopqustuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
	
	# generated password file name
	pw_filename = '.login.pw'

	# generated password folder name
	user_ducc_folder_name = '.ducc'
	
	# generated password file access mode		
	user_ducc_folder_mode = 0700
	
	# ducc.properties key for security home (else use user's home directory)
	key_security_home = 'ducc.security.home'

	# no debug or trace message printed to console
	flag_debug = False
	flag_trace = False
	
	# print debug message
	def debug(self,text):
		if(self.flag_debug):
			print text
	
	# print trace message
	def trace(self,text):
		if(self.flag_trace):
			print text
	
	# display ducc.properties
	def dump_properties(self,props):
		keys = props.get_keys()
		for key in keys:
			value = props.get(key)
			self.trace(key+"="+value)

	# fetch ducc.properties
	def init_props(self):
		self.props = Properties()
		self.props.load(self.path)
		self.dump_properties(self.props)
	
	# determine security home
	def init_security_home(self):
		key = self.key_security_home
		value = self.props.get(key)
		if(value != None):
			self.security_dir = value+'/'+getpass.getuser()
		else:
			self.security_dir = os.path.expanduser("~")
		self.debug("security_dir="+self.security_dir)

	# create security directory
	def init_security_user(self):
		self.user_ducc_dir = self.security_dir+'/'+self.user_ducc_folder_name
		self.debug("user_ducc_dir="+self.user_ducc_dir)
		try:
			os.makedirs(self.user_ducc_dir, self.user_ducc_folder_mode)
		except Exception,e:
			self.debug(str(e))
	
	# create new password
	def pwgen(self):
		count = random.randint(self.pw_len_min,self.pw_len_max)
		pw = ''
		for c in range(0,count):
			imin = 0
			imax = len(self.pw_alphabet)-1
			index = random.randint(imin,imax)
			pwchar = self.pw_alphabet[index]
			text = 'index='+str(index)+' '+pwchar
			self.trace(text)
			pw = pw+pwchar
		self.pw = pw
		self.debug('pw='+self.pw)
	
	# write new password to file
	def pwwrite(self):
		file = self.user_ducc_dir+'/'+self.pw_filename
		self.debug('file='+file)
		with open(file, 'w') as f:
			f.seek(0)
			f.write(self.pw)
			f.write('\n')
			f.truncate()
	
	def main(self, argv):
		try:
			self.debug("DUCC_HOME = "+self.DUCC_HOME)
			self.root = self.DUCC_HOME+'/resources/'
			self.stem = 'ducc.properties'
			self.path = self.root+self.stem
			self.init_props()
			self.init_security_home()
			self.init_security_user()
			self.pwgen()
			self.pwwrite()
		except Exception,e:
			print e

if __name__ == "__main__":
    instance = PwGen()
    instance.main(sys.argv[1:])