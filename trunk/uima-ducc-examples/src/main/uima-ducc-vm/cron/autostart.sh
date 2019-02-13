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

lc=`ps -elf | grep "components=or" | wc -l`

if [ $lc -lt 2 ]
then
        /bin/echo "DUCC is down...reboot!"
        /bin/echo "send e-mail"
        /bin/echo "Subject: uima-ducc-demo reboot!" | /usr/sbin/sendmail lou.degenaro@gmail.com
        /bin/echo "killall -9 java python service"
        /usr/bin/killall -9 java python service >/dev/null 2>&1
        /bin/echo "mkdir logdir"
        /bin/echo "start DUCC"
        cd ~/ducc_runtime/examples/systemtest
        ./start_sim --nothreading -n ducc.nodes -c all
        /bin/sleep 180
        /bin/echo "start registered services"
        cd ~/ducc_runtime/bin
        ./ducc_services --start 0 --role_administrator
        ./ducc_services --start 1 --role_administrator
        ./ducc_services --start 2 --role_administrator
        ./ducc_services --start 3 --role_administrator
        ./ducc_services --start 4 --role_administrator
        ./ducc_services --start 5 --role_administrator
        /bin/sleep 60
        /bin/echo "start workload manager"
        cd ~/ducc_runtime/examples/uima-ducc-vm/driver
        ./driver_start >/dev/null 2>&1
#else
#        /bin/echo "DUCC is running"
fi
