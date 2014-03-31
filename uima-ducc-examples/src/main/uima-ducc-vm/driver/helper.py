#! /usr/bin/env python

import os
import random

class Helper():

    base = '/tmp/ducc/driver'
    
    #####
    
    userDict = {  
        1:'albatross', 2:'bonobo', 3:'chinchilla', 4:'dodo', 5:'eagle', 
        6:'frog', 7:'guppy', 8:'hummingbird', 9:'iguana', 10:'jellyfish',
       11:'kiwi', 12:'lemming', 13:'moose', 14:'nuthatch', 15:'oyster',
       16:'porcupine', 17:'quail', 18:'roadrunner', 19:'squirrel', 20:'tapir',
       21:'urchin', 22:'vicuna', 23:'walrus', 24:'xerus', 25:'yak', 
       26:'zebra'
    }
    
    #####
    
    pctLoClass = 15
    pctHiClass = 15
    
    dictClass = { 1:'low', 2:'normal', 3:'high' }
    
    #####
    
    pctLoItemsNormal = 30
    pctHiItemsNormal = 10
    
    dictItemsNormal = { 1:'1', 2:'2', 3:'3'}
    
    #####
    
    pctError = 10
    
    #####
    
    pctLoItemsError = 20
    pctHiItemsError = 10
    
    dictItemsError = { 1:'4', 2:'4', 3:'6'}
    
    #####
    
    pctLoMemory = 15
    pctHiMemory = 15
    
    dictMemory = { 1:'15', 2:'30', 3:'45'}
    
    #####
    
    dictUnmanagedReservation = { 
        1:'1',
    }
    
    dictManagedReservation = { 
        1:'2', 2:'3',
    }
    
    #####
    
    serviceDict = {
        1:'UIMA-AS:FixedSleepAE_1:tcp://localhost:61617',
        2:'UIMA-AS:FixedSleepAE_4:tcp://localhost:61617',
        3:'UIMA-AS:FixedSleepAE_1:tcp://localhost:61617 UIMA-AS:FixedSleepAE_4:tcp://localhost:61617',
    }
    
    def __init__(self):
        pass
    
    def getUser(self):
        key = random.randint(1, len(self.userDict))
        value = self.userDict.get(key)
        return value
    
    def getClass(self):
        selector = random.randint(1,100)
        if (selector < self.pctLoClass):
            key = 1
        elif (selector < self.pctLoClass+self.pctHiClass):
            key = 3
        else:
            key = 2
        value = self.dictClass.get(key)
        return value

    def getItemsNormal(self):
        selector = random.randint(1,100)
        if (selector < self.pctLoItemsNormal):
            key = 1
        elif (selector < self.pctLoItemsNormal+self.pctHiItemsNormal):
            key = 3
        else:
            key = 2
        value = self.dictItemsNormal.get(key)
        return value

    def getItemsError(self):
        selector = random.randint(1,100)
        if (selector < self.pctLoItemsError):
            key = 1
        elif (selector < self.pctLoItemsError+self.pctHiItemsError):
            key = 3
        else:
            key = 2
        value = self.dictItemsError.get(key)
        return value
            
    def getMemory(self):
        selector = random.randint(1,100)
        if (selector < self.pctLoMemory):
            key = 1
        elif (selector < self.pctLoMemory+self.pctHiMemory):
            key = 3
        else:
            key = 2
        value = self.dictMemory.get(key)
        return value
    
    def getThreads(self):
        selector = random.randint(1,5)
        value = str(2*selector)
        return value
    
    def getJobFileName(self):
        selector = random.randint(1,100)
        if (selector < self.pctError):
            value = self.getItemsError()+'.job'
        else:
            value = self.getItemsNormal()+'.job'
        return value
        
    def getUnmanagedReservationFileName(self):
        key = random.randint(1, len(self.dictUnmanagedReservation))
        value = self.dictUnmanagedReservation.get(key)+'.unmanaged'
        return value
            
    def getManagedReservationFileName(self):
        key = random.randint(1, len(self.dictManagedReservation))
        value = self.dictManagedReservation.get(key)+'.managed'
        return value
            
    def getService(self):
        key = random.randint(1, len(self.serviceDict))
        value = self.serviceDict.get(key)
        return value
    
    def getLogDir(self,user,tid):
        value = self.base+'/'+user+'/ducc/logs'
        if not os.path.exists(value):
            os.makedirs(value)
        return value
        
    def getWorkDir(self,user,tid):
        value = self.base+'/'+user+'/ducc/work'+'/'+tid
        if not os.path.exists(value):
            os.makedirs(value)
        return value
    
    def getHoldTimeInSecondsForManaged(self):
        minMinutes = 30
        maxMinutes = 60
        minutes = random.randint(minMinutes,maxMinutes)
        seconds = minutes * 60
        value = seconds
        return value    
    
    def getHoldTimeInSecondsForUnmanaged(self):
        minHours = 12 
        maxHours = 48
        hours = random.randint(minHours,maxHours)
        minMinutes = 30 
        maxMinutes = 60
        minutes = random.randint(minMinutes,maxMinutes) + (hours * 60)
        seconds = minutes * 60
        value = seconds
        return value
    
if __name__ == '__main__':
    helper = Helper()
    print helper.getUser()
