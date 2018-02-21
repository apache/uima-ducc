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

import datetime
import time

class DuccLogger:

    prefix_debug = 'DEBUG'
    prefix_error = 'ERROR'
    prefix_info  = ' INFO'
    prefix_trace = 'TRACE'
    prefix_warn  = ' WARN'

    def __init__(self):
        self.timestamping_on()
        self.prefixing_on()
        self.debug_off()
        self.error_on()
        self.info_on()
        self.trace_off()
        self.warn_on()
    
    def timestamping_on(self):
        self.flag_timestamping = True
    
    def timestamping_off(self):
        self.flag_timestamping = False
        
    def prefixing_on(self):
        self.flag_prefixing = True
    
    def prefixing_off(self):
        self.flag_prefixing = False
        
    def debug_on(self):
        self.flag_debug = True
        
    def debug_off(self):
        self.flag_debug = False    
    
    def error_on(self):
        self.flag_error = True
        
    def error_off(self):
        self.flag_error = False  
    
    def info_on(self):
        self.flag_info = True
        
    def info_off(self):
        self.flag_info = False  
        
    def trace_on(self):
        self.flag_trace = True
        
    def trace_off(self):
        self.flag_trace = False  
        
    def warn_on(self):
        self.flag_warn = True
        
    def warn_off(self):
        self.flag_warn = False  

    # produce a time stamp
    def get_timestamp(self):
        tod = time.time()
        timestamp = datetime.datetime.fromtimestamp(tod).strftime('%Y-%m-%d %H:%M:%S')      
        return timestamp
       
    # print the message
    def print_message(self,text,prefix,file=None,func=None):
        message = ''
        if(self.flag_timestamping):
            message = message+self.get_timestamp()+' '
        if(self.flag_prefixing):
            message= message+prefix+' '
            if(file != None):
                message = message+file
                if(func != None):
                    message = message+':'+func
                message = message+' '
        message = message+text
        print message   
     
    def debug(self,text,file=None,func=None):
        if(self.flag_debug):
            self.print_message(text,self.prefix_debug,file,func)
         
    def error(self,text,file=None,func=None):
        if(self.flag_error):
            self.print_message(text,self.prefix_error,file,func)
                       
    def info(self,text,file=None,func=None):
        if(self.flag_info):
            self.print_message(text,self.prefix_info,file,func)
                       
    def trace(self,text,file=None,func=None):
        if(self.flag_trace):
            self.print_message(text,self.prefix_trace,file,func)
                           
    def warn(self,text,file=None,func=None):
        if(self.flag_warn):
            self.print_message(text,self.prefix_warn,file,func)        

if __name__ == "__main__":
    base = DuccLogger()

