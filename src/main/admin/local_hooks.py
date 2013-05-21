#!/usr/bin/python

import subprocess
import os
import time
import grp, pwd
import platform
import resource

#
# this is a watson plugin to check_ducc to find java-remote2 processes
#

def find_other_processes(pid, user, line):
    if ( line.find('-DBobsComponent=jr2') >= 0 ):
        return [('java-remote2', pid, user)]

    return [('unknown-java', pid, user)]

#
# This is run via ssh on every node in ducc.properties
#
# This performs watson-local node verification on startup and prevents
# agents from starting on non-viable nodes.
#
# Make sure the new home filesystem is used: 
#       df /home and make sure it is mounted from bluej670
#       Make sure all nfs mounts are good: /data/admin/test-mounts.pl =>
#          this returns OK or problems (From eae' stest-mounts.pl)
#
# Make sure the system clock is close to bluej672
#
# Make sure user and group permissions are good: 
# id ducc => make sure it has group=ducc and also has access to group=saicluster
# id challngr => make sure group=sailimited and has access to group=saicluster
#
# Make sure network DNS is good: ping bluej333 and make sure it is
# trying to: PING bluej333.bluej.net (192.168.2.103) 56(84) bytes of
# data
#
# Make sure prolog is installed ok:
# ls -l /usr/lib/sicstus-4.1.2/bin/jasper.jar => check that it is there
# ldd /usr/lib/libspnative4.1.2.so => check for no errors
#
#
# Make sure /tmp is not full :)
#
def verify_slave_node(localdate, properties):

    # if not ducc, don't enforce sanity, it's testing or a sim
    user = os.environ['LOGNAME']
    bypass = (user != 'ducc')

    if bypass:
        tag = 'NOTE'
    else:
        tag = 'NOTOK'

    # Check mounts and filesystems
    cmd = '/data/admin/test-mounts.pl'
    proc = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    p = proc.stdout
    ok = True
    while 1:
        line = p.readline().strip()
        #print os.uname()[1], line
        if ( not line ):
            break
        if ( line != 'OK' ):
            ok = False

    if ( not ok ):
        print tag, 'Bad mounts on', os.uname()[1]

    # Make sure user ducc and group ducc exist user ducc is in group sailimited
    # Verification of group and user ducc is done while verifying duccling

    if ( user == 'ducc' ):              # in test, we don't want to make this check
        specialgroup = 'sailimited'
        grpinfo = grp.getgrnam('sailimited')
        grmembers = grpinfo.gr_mem
        
        pwinfo = pwd.getpwnam('ducc')
        if ( (grpinfo.gr_gid != pwinfo.pw_gid) and ( not ('ducc' in grmembers) ) ):        
            ok = False
            print tag, 'User ducc in not in group "sailimited"'

    # make sure ping bluej333 is ok
    node = 'bluej333'
    #node = 'bluej658'
    #node = 'bubba'
    cmd = 'ping -c1 ' + node
    proc = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    (sin, serr) = proc.communicate()
    rc = proc.returncode
    if ( rc == 1 ):
        print tag, 'Ping resolves', node, 'but got no reply:'
        ok = False
    elif ( rc == 2 ):
        print tag, 'Cannot ping', node, ':', sin.strip()
        ok = False

    plat = platform.machine()
    # make sure prolog is installed correctly
    # ls -l /usr/lib/sicstus-4.1.2/bin/jasper.jar => check that it is there
    if ( plat == 'x86_64' ):
        prolog_jasper = '/usr/lib/sicstus-4.1.2/bin/jasper.jar'
        prolog_so     = '/usr/lib/libspnative4.1.2.so'
    else:      # ppc64
        prolog_jasper = '/usr/lib/sicstus-4.1.2/bin/jasper.jar'
        prolog_so     = '/usr/lib64/libspnative.so'

    try:
        os.stat(prolog_jasper)
    except:
        print tag, "Cannot find", prolog_jasper 
        ok = False

    cmd = 'ldd ' + prolog_so
    proc = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    (sin, sout) = proc.communicate()
    rc = proc.returncode

    if ( rc != 0 ):
        print tag, "Bad or missing prolog lib:", sin.strip()
        ok = False
    else:
        lines = sin.split('\n')
        for l in lines:
            if ( l.find('not found') >= 0 ):
                print tag, "Problem in ", prolog_so, ":", l.strip()
                ok = False

    # make sure /tmp is not full
    cmd = 'df -Pm /tmp'
    proc = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    (sin, sout) = proc.communicate()
    rc = proc.returncode
    mintmp = 2000 # in mb
    for l in sin.split('\n'):
       if ( l.startswith('/') ):
           toks = l.split()
           if ( int(toks[3]) <= mintmp ):
               print tag, '/tmp space is less than minimum of', int(mintmp), ":", line
               ok = False

    # verify java.  A hello must have been compiled by the master for this to work
    here = os.getcwd()
    os.chdir(os.environ['DUCC_HOME'] + '/admin')
    java = properties.get('ducc.jvm')
    if ( java == None ):
        print tag, 'WARN: "ducc.jvm" is not configured, using "java" instead.'
        java = 'java'
    cmd = java + ' DuccHello'
    proc = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
    (sin, sout) = proc.communicate()
    rc = proc.returncode
    os.chdir(here)
    if ( sin.strip() != 'hiya' ):
        print tag, 'Cannot run java, HelloWorld failed.'
        ok = False

    # check rlimits - rss and virtual must be unlimited
    (softrss , hardrss)  = resource.getrlimit(resource.RLIMIT_RSS)
    (softvmem, hardvmem) = resource.getrlimit(resource.RLIMIT_AS)
    if ( softrss != -1 ):
        print tag, 'RSS limit is not unlimited:', softrss
        ok = False
    if ( softvmem != -1 ):
        print tag, 'VMEM limit is not unlimited:', softvmem
        ok = False
        
    return ok or bypass

#
# This is run on the master node (the "ducc head") before any of the verify_slave_node
# calls are made, to allow common setup or special tests
#
def verify_master_node(properties):
    return True
