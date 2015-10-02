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

from threading import Thread, BoundedSemaphore
from random import randint
import time
import os

from helper import Helper
from config import Config

import subprocess
        
class ReservationType():
    Managed = 1
    Unmanaged = 2
    
class RunReservation(Thread,Config):

    helper = Helper();
    
    def __init__(self,permit,tid,reservationType):
        Thread.__init__(self)
        self.permit = permit
        self.tid = str(tid)
        print self.helper.timestamp(),'init:'+str(self.tid)
        self.reservationType = reservationType

    def sleep(self):
        sleepTime = randint(1,300)
        print self.helper.timestamp(),self.getName(),' seconds to sleep: ',str(sleepTime)
        time.sleep(sleepTime)
    
    def hold(self):
        if (self.reservationType == ReservationType.Managed):
            sleepTime = self.helper.getHoldTimeInSecondsForManaged()
        else:
            sleepTime = self.helper.getHoldTimeInSecondsForUnmanaged()
        print self.helper.timestamp(),self.getName(),' seconds to hold: ',str(sleepTime)
        time.sleep(sleepTime)
    
    def process(self):
        self.user = self.helper.getUser()
        print self.helper.timestamp(),self.getName(),' user: ',self.user
        if (self.reservationType == ReservationType.Managed):
            command = self.manSubmit
            fileName = self.reservations+'/'+self.helper.getManagedReservationFileName()
        else:
            command = self.resSubmit
            fileName = self.reservations+'/'+self.helper.getUnmanagedReservationFileName()
        spArgs = []
        spArgs.append(command)
        spArgs.append('-f')
        spArgs.append(fileName)
        print self.helper.timestamp(),self.getName(),' args: ',spArgs
        os.environ['USER'] = self.user
        sp = subprocess.Popen(spArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        tokens = out.split(' ')
        #print tokens
        if (self.reservationType == ReservationType.Managed):
            self.rid = tokens[2]
        else:
            self.rid = tokens[1]
        print self.helper.timestamp(),self.getName(),' id='+self.rid
        #print 'err='+err
        
    def cleanup(self):
        if (self.reservationType == ReservationType.Managed):
            command = self.manCancel
        else:
            command = self.resCancel
        spArgs = []
        spArgs.append(command)
        spArgs.append('-id')
        spArgs.append(self.rid)
        print self.helper.timestamp(),self.getName(),' args: ',spArgs
        os.environ['USER'] = self.user
        sp = subprocess.Popen(spArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        print self.helper.timestamp(),self.getName(),' out: ',out
        #print 'err='+err
        
    def run(self):
        print self.helper.timestamp(),'run.start:'+str(self.tid)
        while True:
            self.permit.acquire()
            self.sleep()
            self.process()
            self.hold()
            self.cleanup()   
            self.permit.release()  
        print self.helper.timestamp(),'run.end:'+str(self.tid)
       
class RunJob(Thread,Config):

    helper = Helper();
    
    def __init__(self,permit,tid):
        Thread.__init__(self)
        self.permit = permit
        self.tid = str(tid)
        print self.helper.timestamp(),'init:'+str(self.tid)

    def sleep(self):
        sleepTime = randint(1,300)
        print self.helper.timestamp(),self.getName(),' seconds to sleep: ',str(sleepTime)
        time.sleep(sleepTime)

    def process(self):
        self.user = self.helper.getUser()
        print self.helper.timestamp(),self.getName(),' user: ',self.user
        spArgs = []
        spArgs.append(self.jobSubmit)
        spArgs.append('--wait_for_completion')
        spArgs.append('--scheduling_class')
        spArgs.append(self.helper.getClass())
        spArgs.append('--process_memory_size')
        spArgs.append(self.helper.getMemory())
        spArgs.append('--process_pipeline_count')
        spArgs.append(self.helper.getThreads())
        spArgs.append('--log_directory')
        subdir = str(randint(1,1000000000))
        spArgs.append(self.helper.getLogDir(self.user, subdir))
        spArgs.append('--working_directory')
        spArgs.append(self.helper.getWorkDir(self.user, subdir))
        if(randint(0,1) > 0):
            spArgs.append('--service_dependency')
            spArgs.append(self.helper.getServiceSet())
        spArgs.append('-f')
        jobFileName = self.helper.getJobFileName()
        job = self.jobs+'/'+jobFileName
        spArgs.append(job)
        print self.helper.timestamp(),self.getName(),' args: ',spArgs
        os.environ['USER'] = self.user
        sp = subprocess.Popen(spArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        tokens = out.split(' ')
        #print tokens
        self.jid = tokens[1]
        print self.helper.timestamp(),self.getName(),' out: ',out
        if err:
            print err

    def cleanup(self):
        print self.helper.timestamp(),self.getName(),' user: ',self.user
        spArgs = []
        spArgs.append(self.jobCancel)
        spArgs.append('-id')
        spArgs.append(self.jid)
        print self.helper.timestamp(),self.getName(),' args: ',spArgs
        os.environ['USER'] = self.user
        sp = subprocess.Popen(spArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        print self.helper.timestamp(),self.getName(),' out: ',out
        #print 'err='+err
        
    def run(self):
        print self.helper.timestamp(),'run.start:'+str(self.tid)
        while True:
            self.permit.acquire()
            self.sleep()
            self.process()
            self.cleanup()
            self.permit.release()
        print self.helper.timestamp(),'run.end:'+str(self.tid)
    
if __name__ == '__main__':

    print 'driver.start'
    
    permitForJobs = BoundedSemaphore(3)
    permitForReservations = BoundedSemaphore(2)
    
    tid = 0
    
    tid += 1
    th1 = RunJob(permitForJobs, tid)
    th1.start()
    
    tid += 1
    th2 = RunJob(permitForJobs, tid)
    th2.start()
    
    tid += 1
    th3 = RunJob(permitForJobs, tid)
    th3.start()
    
    tid += 1
    th4 = RunReservation(permitForReservations,tid,ReservationType.Unmanaged)
    th4.start()
    
    tid += 1
    th5 = RunReservation(permitForReservations,tid,ReservationType.Managed)
    th5.start()
        
    th1.join()
    th2.join()
    th3.join()
    th4.join()
    th5.join()
    
    print 'driver.end'
