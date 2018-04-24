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

# Example script to run 2 DUCC jobs, sequentially

import os
import inspect

# determine ducc_home, relatively
ducc_home = os.path.dirname(os.path.abspath(inspect.stack()[0][1]))[:-12]

# calculate ducc_submit command path
cmd_submit = os.path.join(ducc_home,'bin/ducc_submit')

jobA = 'examples/simple/1.job'
jobB = 'examples/simple/1.dd.job'

# construct path to example 1.job and 1.dd.job
file_jobA = os.path.join(ducc_home,jobA)
file_jobB = os.path.join(ducc_home,jobB)

# construct submit commands for 1.job and 1.dd.job
run_jobA = cmd_submit+' -f '+file_jobA+' '+'--wait_for_completion'
run_jobB = cmd_submit+' -f '+file_jobB+' '+'--wait_for_completion'

# submit 1.job and wait for completion
print 'jobA: submit '+jobA
result = os.system(run_jobA)
print 'jobA: '+str(result)

# can't get here until 1.job is finished

# submit 1.dd.job and wait for completion
print 'jobB: submit '+jobB
result = os.system(run_jobA)
print 'jobB: '+str(result)

# can't get here until 1.dd.ob is finished

