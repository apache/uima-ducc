#! /usr/bin/env python

import os
import sys

class Config():
    
    driver_home = os.path.dirname(os.path.realpath(sys.argv[0]))
    ducc_home = driver_home.rsplit('/',3)[0]
    examples = ducc_home+'/examples/uima-ducc-vm'
    jobs = examples+'/jobs'
    reservations = examples+'/reservations'
    jobSubmit = ducc_home+'/bin/ducc_submit'
    resSubmit = ducc_home+'/bin/ducc_reserve'
    resCancel = ducc_home+'/bin/ducc_unreserve'
    manSubmit = ducc_home+'/bin/ducc_process_submit'
    manCancel = ducc_home+'/bin/ducc_process_cancel'