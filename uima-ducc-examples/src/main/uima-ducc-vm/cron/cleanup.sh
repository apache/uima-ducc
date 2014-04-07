#!/bin/bash
for f in `find /tmp -maxdepth 1 -mtime +7`; do rm -rf $f; done
mkdir /tmp/degenaro
mkdir /tmp/degenaro/ducc
mkdir /tmp/degenaro/ducc/logs