#!/usr/bin/python

#
# Returns a list of 3-tuples of processes other than known ducc processes, for
# display by check_ducc.  Each 3-tupls is of the form
#   (proccessnamd, pid, user)
# where
#   processid is the name of the process (preferably the short name, like 'java')
#   pid is the pid of the process
#   user is the owner of the process
#
def find_other_processes(pid, user, line):
    return []

# 
# This performs any installation-wise checking needed on each of the slave ducc nodes.
# If False is returned, ducc will not start an agent on the node.
def verify_slave_node(localdate, properties):
    return True

#
# This performs any installation-wise chacking on the master ducc node.
# If False is returned, ducc will not start.
#
def verify_master_node(properties):
    return True
