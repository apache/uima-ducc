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

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +
# + ducc_util_out
# +
# + purpose: ducc utility for output
# + 
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import datetime
import time

flag_debug = False
flag_error = True
flag_info  = False
flag_trace = False
flag_warn  = True

prefix_debug = 'DEBUG'
prefix_error = 'ERROR'
prefix_info  = ' INFO'
prefix_trace = 'TRACE'
prefix_warn  = ' WARN'

flag_timestamping = True
flag_prefixing = True

def timestamping_on():
    global flag_timestamping
    flag_timestamping = True
    
def timestamping_off():
    global flag_timestamping
    flag_timestamping = False

def prefixing_on():
    global flag_prefixing
    flag_prefixing = True
    
def prefixing_off():
    global flag_prefixing
    flag_prefixing = False  

def debug_on():
    global flag_debug
    flag_debug = True
    
def debug_off():
    global flag_debug
    flag_debug = False    

def error_on():
    global flag_error
    flag_error = True
    
def error_off():
    global flag_error
    flag_error = False  

def info_on():
    global flag_info
    flag_info = True
    
def info_off():
    global flag_info
    flag_info = False  
    
def trace_on():
    global flag_trace
    flag_trace = True
    
def trace_off():
    global flag_trace
    flag_trace = False  
    
def warn_on():
    global flag_warn
    flag_warn = True
    
def warn_off():
    global flag_warn
    flag_warn = False  

# produce a time stamp
def get_timestamp():
    tod = time.time()
    timestamp = datetime.datetime.fromtimestamp(tod).strftime('%Y-%m-%d %H:%M:%S')      
    return timestamp

# print the message
def print_message(text,prefix):
    global flag_timestamping
    global flag_prefixing
    message = ''
    if(flag_timestamping):
        message = message+get_timestamp()+' '
    if(flag_prefixing):
        message= message+prefix+' '
    message = message+text
    print message
    
def print_debug(text):
    global flag_debug
    global prefix_debug
    if(flag_debug):
        print_message(text,prefix_debug)

def print_error(text):
    global flag_error
    global prefix_error
    if(flag_error):
        print_message(text,prefix_error)

def print_info(text):
    global flag_info
    global prefix_info
    if(flag_info):
        print_message(text,prefix_info)

def print_trace(text):
    global flag_trace
    global prefix_trace
    if(flag_trace):
        print_message(text,prefix_trace)

def print_warn(text):
    global flag_warn
    global prefix_warn
    if(flag_warn):
        print_message(text,prefix_warn)
        