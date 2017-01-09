#!/usr/bin/python
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


#
# Returns a list of 3-tuples of processes other than known ducc processes, for
# display by check_ducc.  Each 3-tupls is of the form
#   (proccessnamd, pid, user)
# where
#   processid is the name of the process (preferably the short name, like 'java')
#   pid is the pid of the process
#   user is the owner of the process
#
def find_other_processes(pid, user, line):
    return []

# 
# This performs any installation-wise checking needed on each of the slave ducc nodes.
# If False is returned, ducc will not start an agent on the node.
def verify_slave_node(localdate, properties):
    return True

#
# This performs any installation-wise chacking on the master ducc node.
# If False is returned, ducc will not start.
#
def verify_master_node(properties):
    return True
