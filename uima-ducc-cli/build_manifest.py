#!/usr/bin/env python

#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   #  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.

# THis provides a way to list the manifest jars vertically since it seems like mvn corrupts the
# manifest if they're listed like this in the pom directly.
#
# Run this script and c/p the stdout into the DUCC_CP property in the pom.
#
def main():

    cp = [ \
        "uima-ducc-common.jar",
        "uima-ducc-transport.jar",
        "../resources/",
        "slf4j/jcl-over-slf4j.jar",
        "slf4j/slf4j-api.jar",
        "slf4j/slf4j-log4j12.jar",
        "xstream/xstream.jar",
        "http-client/commons-codec-${commons.codec.version}.jar",
        "http-client/commons-httpclient-${commons.httpclient.version}.jar",
        "google-gson/gson-${google.gson.version}.jar",
        "apache-log4j/log4j-${log4j.version}.jar",
        "apache-commons-cli/commons-cli-${commons.cli.version}.jar",
        "uima/uima-core-${org.apache.uima.version}.jar",
        "uima/uimaj-as-core-${org.apache.uima.as.version}.jar",
        ]

    print ' '.join(cp)

main()
