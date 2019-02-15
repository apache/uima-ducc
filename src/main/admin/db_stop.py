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
import time
import traceback

from ducc_util import DuccUtil

# command to stop DB on local head node, regardless of automanage


class DbStop(DuccUtil):
	
	def __init__(self):
		DuccUtil.__init__(self, True)
	
	# extra help!
	def get_epilog(self):
		epilog = ''
		epilog = epilog+'Purpose: stop database on local node, regardless of automanage.'
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
			self.db_process_kill(15)
			if(self.db_process_alive()):
				self.debug('sleeping...')
				time.sleep(10)
				if(self.db_process_alive()):
					self.debug('forcing...')
					self.db_process_kill(9)
			if(self.db_process_alive()):
				self.debug('still alive.')
				rc = 1
		except Exception as e:
			rc = 1
			traceback.print_exc()
		sys.exit(rc)
		
if __name__ == '__main__':

	instance = DbStop()
	instance.main(sys.argv[1:])
