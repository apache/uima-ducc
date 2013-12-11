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

# simple bootstratp to establish DUCC_HOME and to set the python path so it can
# find the common code in DUCC_HOME/admin
# Infer DUCC_HOME from our location - no longer use a (possibly inaccurate) environment variable
def set_ducc_home():
    me = os.path.abspath(sys.argv[0])    
    ndx = me.rindex('/')
    ndx = me.rindex('/', 0, ndx)
    DUCC_HOME = me[:ndx]          # split from 0 to ndx

    sys.path.append(DUCC_HOME + '/admin')

