#!/bin/bash
for f in `find /tmp -maxdepth 1 -mtime +7`; do rm -rf $f; done