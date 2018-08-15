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
import os
import subprocess

from ducc_util import DuccUtil

# command to query the database for all started daemons (i.e. what the autostart.py command sees)

class AutostartQuery(DuccUtil):

    description = 'List the entries from the autostart database table.'

    def get_args(self):
        parser = argparse.ArgumentParser(description=self.description)
        self.args = parser.parse_args()

    def main(self, argv):    
        self.get_args()
        jclass = 'org.apache.uima.ducc.database.lifetime.DbDaemonLifetimeUI'
        option = '--query'
        cmd = [self.jvm, '-DDUCC_HOME='+self.DUCC_HOME, jclass, option]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        #print out
        lines = out.split('\n')
        counter = 0
        for line in lines:
            if('.db' in line):
                print line
                counter = counter + 1
            elif('.br' in line):
                print line
                counter = counter + 1
            elif('.or' in line):
                print line
                counter = counter + 1
            elif('.pm' in line):
                print line
                counter = counter + 1
            elif('.rm' in line):
                print line
                counter = counter + 1
            elif('.sm' in line):
                print line
                counter = counter + 1
            elif('.ws' in line):
                print line
                counter = counter + 1
            elif('.ag' in line):
                print line
                counter = counter + 1
        if(counter == 0):
            print 'no daemon(s) registered as started in database'
        
if __name__ == "__main__":

    instance = AutostartQuery()
    instance.main(sys.argv[1:])
