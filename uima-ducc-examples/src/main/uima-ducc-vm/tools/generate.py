#! /usr/bin/env python

import random

if __name__ == '__main__':
    min = 10
    max = 60
    mils = 1000
    count = 101
    list = ''
    i = 0
    for t in range(0,count):
        secs = random.randint(min*mils,max*mils)
        list += str(secs)+' '
        i += 1
    print i
    print list