#!/usr/bin/env python

import os
import sys
import getopt
import string
import time
import subprocess
import shutil
import signal
from threading import *
import Queue

if ( not os.environ.has_key('DUCC_HOME') ):    
    me = os.path.abspath(sys.argv[0])

    print "ME:", me
    ndx = me.rindex('/')
    ndx = me.rindex('/', 0, ndx)
    ndx = me.rindex('/', 0, ndx)

    DUCC_HOME=me[:ndx]
    os.environ['DUCC_HOME'] = DUCC_HOME
    print 'Environment variable DUCC_HOME is missing, using default', DUCC_HOME
else:
    DUCC_HOME = os.environ['DUCC_HOME']

print 'Using', os.environ['DUCC_HOME']
sys.path.append(DUCC_HOME + '/admin')

from ducc_util import DuccUtil
from ducc_util import DuccProperties
from ducc      import Ducc

service = None
def handler(signal, frame):
    global service
    print 'Caught signal', signal
    service.terminate()

class DuccService(DuccUtil):


    def format_cp(self, cp):
        for i in cp.split(':'):
            print i

    def terminate(self):
        self.terminated = True
        service.send_signal(2)

    def start_uima_as_service(self, instance):
        global service
        self.terminated = False

        service_cp = os.environ['CLASSPATH'] + ':' + self.DUCC_HOME + '/examples/lib/uima-ducc-examples.jar'
        service_cp = service_cp              + ':' + self.DUCC_HOME + '/lib/uima/*'
        #self.format_cp(service_cp)

	print 'broker:', self.broker_url

        CMD = ['java']
        CMD.append("-DSERVICE_ID=" + instance)
        CMD.append("-DdefaultBrokerURL='" + self.broker_url + "'")
        CMD.append('-DJAVA_HOME=/share/jdk1.6')
        CMD.append('-classpath ' + service_cp)
        CMD.append('org.apache.uima.ducc.test.service.UIMA_Service')
        CMD.append('-saxonURL file:' + DUCC_HOME + '/lib/saxon/saxon8.jar')
        CMD.append('-xslt ' + DUCC_HOME + '/resources/dd2spring.xsl')
        CMD.append('-dd ' + DUCC_HOME + '/examples/simple/resources/Service_FixedSleep_' + instance + '.xml')
        #CMD.append('-b')
        os.environ['AE_INIT_TIME'] = '5'
        os.environ['AE_INIT_RANGE'] = '5'

        service = subprocess.Popen(' '.join(CMD), bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        p = service.stdout
        while 1:
            line = p.readline().strip()
            if ( not line ):
                return

            print '[]',line

        print '------------------------- UIMA_AS Service Pid', self.service_pid
    
    def usage(self, *msg):
        print msg
        if ( msg[0] != None ):
            print ' '.join(msg)

        print 'Usage:'
        print '   service.py [optons]'
        print ''
        print 'Where options are'
        print '   -i instance_number'
        print ''
        print 'We run with DUCC_HOME set to', os.environ['DUCC_HOME']
        sys.exit(1)
    
    def main(self, argv):
        global DUCC_HOME
        global DUCC_TOP

        self.instance = None
        opts, args  = getopt.getopt(argv, 'i::?h', ['instance='])
    
        for ( o, a ) in opts:
            if o in ('-i', '--instance'):
                self.instance = a        
    
        if ( self.instance == None ):
            self.usage(None)

        self.start_uima_as_service(self.instance)

# --------------------------------------------------------------------------------
if __name__ == "__main__":

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGQUIT, handler)
    svc = DuccService()

    svc.set_classpath()
    svc.main(sys.argv[1:])
