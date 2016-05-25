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
import sys

class Config():
    
    driver_home = os.path.dirname(os.path.realpath(sys.argv[0]))
    ducc_home = driver_home.rsplit('/',3)[0]
    examples = ducc_home+'/examples/uima-ducc-vm'
    jobs = examples+'/jobs'
    reservations = examples+'/reservations'
    jobSubmit = ducc_home+'/bin/ducc_submit'
    jobCancel = ducc_home+'/bin/ducc_cancel'
    resSubmit = ducc_home+'/bin/ducc_reserve'
    resCancel = ducc_home+'/bin/ducc_unreserve'
    manSubmit = ducc_home+'/bin/ducc_process_submit'
    manCancel = ducc_home+'/bin/ducc_process_cancel'