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
# 	 http://www.apache.org/licenses/LICENSE-2.0
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
	print('Python minimum requirement is version ' + str(version_min[0]) + '.' + str(version_min[1]))
	sys.exit(1)

import argparse
from argparse import RawDescriptionHelpFormatter
import os
import subprocess
import traceback

from ducc_util import DuccUtil

# command to start DB on local head node, regardless of automanage

class DbStart(DuccUtil):
	
	def __init__(self):
		DuccUtil.__init__(self, True)
	
	# extra help!
	def get_epilog(self):
		epilog = ''
		epilog = epilog+'Purpose: start database on local node, regardless of automanage.'
		return epilog
	
	help_debug = 'Display debugging messages.'
	
	def debug(self,text):
		if(self.args.debug):
			if(text != None):
				print text
	
	def main(self, argv):	
		rc = 0
		try:
			self.parser = argparse.ArgumentParser(formatter_class=RawDescriptionHelpFormatter,epilog=self.get_epilog())
			self.parser.add_argument('--debug', '-d', action='store_true', help=self.help_debug)
			self.args = self.parser.parse_args()
			if(self.db_process_alive()):
				self.debug('already running')
			else:
				here = os.getcwd()
				self.verify_limits()
				xmx = self.ducc_properties.get('ducc.database.mem.heap')
				new = self.ducc_properties.get('ducc.database.mem.new')
				if ( not ( xmx == None and new == None ) ):   # if either is set
					if ( xmx == None or new == None ) :	  # then both must be set
						self.debug('Database properties ducc.database.mem.heap and ducc.database.mem.new must both be set.')
						rc = 1
					else:
						os.environ['MAX_HEAP_SIZE'] = xmx
						os.environ['HEAP_NEWSIZE'] = new
				if(rc == 0):
					jmxport = self.ducc_properties.get('ducc.database.jmx.port')
					if ( jmxport != None ):
						os.environ['JMX_PORT'] = jmxport
					jmxhost = self.ducc_properties.get('ducc.database.jmx.host')
					if ( jmxhost != None and jmxhost != 'localhost' ):
						os.environ['LOCAL_JMX'] = 'no'
					os.chdir(self.DUCC_HOME + '/cassandra-server')
					cmd = 'bin/cassandra -p ' + self.db_pidfile + ' > ' + self.db_logfile + ' 2>&1'
					self.debug(cmd)
					os.system(cmd);
		except Exception as e:
			rc = 1
			traceback.print_exc()
		sys.exit(rc)
		
if __name__ == '__main__':

	instance = DbStart()
	instance.main(sys.argv[1:])
