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

import time
import sys

import logging
import stomp

from ducc_boot import *
set_ducc_home()

from ducc_util import DuccUtil

class DuccStomp(DuccUtil):

    def connect(self, host, port, listener):
        self.host = host
        self.port = port
        self.listener = listener
        logging.basicConfig()

        conn = stomp.Connection( [(host, port)])      # list of host, port tuples
        conn.set_listener('', listener)
        conn.start()
        conn.connect()
        self.conn = conn

    def close(self):
        try:
            self.conn.disconnect()
        except:
            sys.exit(0)

    def subscribe(self, endpoint):
        self.conn.subscribe(destination=endpoint, ack='auto')
