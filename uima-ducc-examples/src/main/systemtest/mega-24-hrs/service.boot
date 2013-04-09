
# submit these services, 2 instances each
#submit  0 1
#instances_0 4
#instances_1 4

# register these services, 2 instances each
register 2 3 4 5 
instances_2 4
instances_3 4
instances_4 4
instances_5 4

# start these registered services
start 2 3

# start 2 standalone services
standalone 6 7 
instances_6 4
instances_7 4

