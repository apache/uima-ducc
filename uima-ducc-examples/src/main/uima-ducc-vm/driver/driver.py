#! /usr/bin/env python

from threading import Thread
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
    
    def __init__(self,tid,reservationType):
        Thread.__init__(self)
        self.tid = str(tid)
        print 'init:'+str(self.tid)
        self.reservationType = reservationType

    def sleep(self):
        sleepTime = randint(1,300)
        print self.getName()+' seconds to sleep: '+str(sleepTime)
        time.sleep(sleepTime)
    
    def hold(self):
        if (self.reservationType == ReservationType.Managed):
            sleepTime = self.helper.getHoldTimeInSecondsForManaged()
        else:
            sleepTime = self.helper.getHoldTimeInSecondsForUnmanaged()
        print self.getName()+' seconds to hold: '+str(sleepTime)
        time.sleep(sleepTime)
    
    def process(self):
        self.user = self.helper.getUser()
        print self.user
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
        print spArgs
        os.environ['USER'] = self.user
        sp = subprocess.Popen(spArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        tokens = out.split(' ')
        #print tokens
        if (self.reservationType == ReservationType.Managed):
            self.rid = tokens[2]
        else:
            self.rid = tokens[1]
        print 'id='+self.rid
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
        print spArgs
        os.environ['USER'] = self.user
        sp = subprocess.Popen(spArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        print out
        #print 'err='+err
        
    def run(self):
        print 'run.start:'+str(self.tid)
        while True:
            self.sleep()
            self.process()
            self.hold()
            self.cleanup()     
        print 'run.end:'+str(self.tid)
       
class RunJob(Thread,Config):

    helper = Helper();
    
    def __init__(self,tid):
        Thread.__init__(self)
        self.tid = str(tid)
        print 'init:'+str(self.tid)

    def sleep(self):
        sleepTime = randint(1,300)
        print self.getName()+' seconds to sleep: '+str(sleepTime)
        time.sleep(sleepTime)

    def process(self):
        self.user = self.helper.getUser()
        print self.user
        spArgs = []
        spArgs.append(self.jobSubmit)
        spArgs.append('--wait_for_completion')
        spArgs.append('--scheduling_class')
        spArgs.append(self.helper.getClass())
        spArgs.append('--process_memory_size')
        spArgs.append(self.helper.getMemory())
        spArgs.append('--process_thread_count')
        spArgs.append(self.helper.getThreads())
        spArgs.append('--log_directory')
        spArgs.append(self.helper.getLogDir(self.user, self.tid))
        spArgs.append('--working_directory')
        spArgs.append(self.helper.getWorkDir(self.user, self.tid))
        if(randint(0,1) > 0):
            spArgs.append('--service_dependency')
            spArgs.append(self.helper.getServiceSet())
        spArgs.append('-f')
        jobFileName = self.helper.getJobFileName()
        job = self.jobs+'/'+jobFileName
        spArgs.append(job)
        print spArgs
        os.environ['USER'] = self.user
        sp = subprocess.Popen(spArgs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        print out
        if err:
            print err

    def cleanup(self):
        pass

    def run(self):
        print 'run.start:'+str(self.tid)
        while True:
            self.sleep()
            self.process()
            self.cleanup()
        print 'run.end:'+str(self.tid)
    
if __name__ == '__main__':

    print 'driver.start'
    
    tid = 0
    
    tid += 1
    th1 = RunJob(tid)
    th1.start()
    
    tid += 1
    th2 = RunJob(tid)
    th2.start()
    
    tid += 1
    th3 = RunJob(tid)
    th3.start()
    
    tid += 1
    th4 = RunReservation(tid,ReservationType.Unmanaged)
    th4.start()
    
    tid += 1
    th5 = RunReservation(tid,ReservationType.Managed)
    th5.start()
        
    th1.join()
    th2.join()
    th3.join()
    th4.join()
    th5.join()
    
    print 'driver.end'
