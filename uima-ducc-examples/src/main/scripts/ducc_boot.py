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
def set_ducc_home():
    if ( os.environ.has_key('DUCC_HOME') ):
        DUCC_HOME = os.environ['DUCC_HOME']
    else:
        me = os.path.abspath(sys.argv[0])    
        ndx = me.rindex('/')
        ndx = me.rindex('/', 0, ndx)
        ndx = me.rindex('/', 0, ndx)
        DUCC_HOME = me[:ndx]          # split from 0 to ndx
        os.environ['DUCC_HOME'] = DUCC_HOME

    sys.path.append(DUCC_HOME + '/admin')

