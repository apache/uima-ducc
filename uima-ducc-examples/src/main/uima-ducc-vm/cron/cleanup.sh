#!/bin/bash
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

# this section reports what the script will remove
togo=`find /tmp/degenaro/ducc/logs/ -maxdepth 1 -name "[0-9]*"  -mtime +2 | wc -l`
echo Removing $togo directories in /tmp/degenaro/ducc/logs/
find /tmp/degenaro/ducc/logs/ -maxdepth 1 -name "[0-9]*"  -mtime +2 | xargs -i ls -ld {}
togo=`find /home/degenaro/ducc/logs/ -maxdepth 1 -name "[0-9]*"  -mtime +2 | wc -l`
echo Removing $togo directories in /home/degenaro/ducc/logs/
find /home/degenaro/ducc/logs/ -maxdepth 1 -name "[0-9]*"  -mtime +2 | xargs -i ls -ld {}
togo=`find /tmp/ducc/driver/ -maxdepth 4 -name "[0-9]*"  -mtime +1 | wc -l`
echo Removing $togo directories in /tmp/ducc/driver/
find /tmp/ducc/driver/ -maxdepth 4 -name "[0-9]*"  -mtime +1 | xargs -i ls -ld {}


# this section actually removes stuff
find /tmp/degenaro/ducc/logs/ -maxdepth 1 -name "[0-9]*"  -mtime +2 | xargs -i rm -rf {}
find /home/degenaro/ducc/logs/ -maxdepth 1 -name "[0-9]*"  -mtime +2 | xargs -i rm -rf {}
find /tmp/ducc/driver/ -maxdepth 4 -name "[0-9]*"  -mtime +1 | xargs -i rm -rf {}