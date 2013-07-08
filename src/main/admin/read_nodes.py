#!/usr/bin/env python
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


import os
import sys

from ducc_util import DuccUtil
from ducc_base import DuccProperties

#
# Read the ducc node list and spew to stdout - handles comments, imports, etc.
#
class ReadNodes(DuccUtil):

    def main(self, argv):
        nodes = {}
        nodes = self.read_nodefile(argv[0], nodes)
        for ( nodefile, nodelist ) in nodes.items():
            for host in nodelist:
                print host

if __name__ == "__main__":
    if (len(sys.argv) == 1 ):
        print "Usage: read_nodes <nodefile>"
        sys.exit(1)
        
    reader = ReadNodes()
    reader.main(sys.argv[1:])
    
