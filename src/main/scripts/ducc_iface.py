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

'''
Created on Sep 28, 2011
'''
    
import os
import sys
import subprocess

class DuccIface:
    
    def cpAppend(self,cp,append):
        if( self.debug ):
            print "append="+append
        if ( cp == "" ):
            return append   
        
        else:
            return cp+":"+ append
                            
    def addJarsOpen1(self,cp,lib):
        for libdir in self.libdirs1:
            pathdir = os.path.join(lib,libdir)
            for libfile in os.listdir(pathdir):
                if ( libfile.endswith(".jar") ):
                    pathjar = os.path.join(pathdir,libfile)
                    cp = self.cpAppend(cp,pathjar)
        return cp

    def addJarsDucc(self,cp,lib):
        for jar in self.duccjars:
            pathjar = os.path.join(lib,jar)
            cp = self.cpAppend(cp,pathjar)
        return cp 
    
    def __init__(self):
        return
    
    def initialize(self):
        
        self.debug = False
        
        self.jproperties = "-Dducc.deploy.configuration=resources/ducc.properties"

        self.duccjars = [
                    "ducc-agent.jar",
                    "ducc-cli.jar", 
                    "ducc-common.jar",
                    "ducc-jd.jar",
                    "ducc-orchestrator.jar",
                    "ducc-pm.jar",
                    "ducc-rm.jar",
                    "ducc-sm.jar",
                    "ducc-web.jar", 
                    ]

        self.libdirs1 = [ 
                   "apache-activemq-5.5.0",
                   "apache-commons-cli-1.2",
                   "apache-commons-lang-2.6",
                   "guava-r09",
                   "apache-log4j-1.2.16",
                   "uima-as.2.3.1",
                   "apache-camel-2.7.1",
                   "apache-commons-collections-3.2.1",
                   "joda-time-1.6",
                   "springframework-3.0.5",
                   "xmlbeans-2.5.0",
                   "bluepages",
                   ]
                    
        ducc_home = os.environ.get("DUCC_HOME", "")
        
        self.lib = os.path.join(ducc_home,"lib")
        if ( self.debug ):
            print "LIB="+self.lib
        
        self.cp = ""
        self.cp = self.addJarsDucc(self.cp,self.lib)
        self.cp = self.addJarsOpen1(self.cp,self.lib)
        self.cp = self.cpAppend(self.cp,os.path.join(ducc_home,"resources"))
        if ( self.debug ):
            print "CLASSPATH="+self.cp
        
        return
    
    def main(self,argv,pclass,jclass):
        ducc_home = os.environ.get("DUCC_HOME", "")
        if(ducc_home == ""):
            print "DUCC_HOME not set in environment"
            sys.exit(1)
        jvm_dir = os.environ.get("JVM_DIR", "")

        if(jvm_dir == ""):
            jvm_dir = os.environ.get("JAVA_HOME", "")
            if ( jvm_dir == "" ):
                print 'JAVA_HOME is not set in the environment'
                sys.exit(1)
            else:
                jvm = jvm_dir + os.sep + '/bin/java'
        else:
            jvm = jvm_dir + os.sep + 'java'


        self.initialize()
        jcmd = "-classpath "+self.cp+" "+self.jproperties+" "+jclass
        jcmdargs = ""
        for arg in argv:
            jcmdargs += " "+arg
        cmd = jvm+" "+jcmd+" "+jcmdargs
        if(self.debug):
            print cmd
        
        process = subprocess.Popen(cmd, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        stdout, stderr = process.communicate()

        print stdout
        if stderr != "":
            print stderr

        return
