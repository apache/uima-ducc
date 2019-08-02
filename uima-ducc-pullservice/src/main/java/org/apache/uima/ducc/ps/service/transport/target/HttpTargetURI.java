/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.uima.ducc.ps.service.transport.target;

import org.apache.uima.ducc.ps.service.transport.ITargetURI;

public class HttpTargetURI implements ITargetURI {

  String target;

  String targetContext;

  String targetPort;

  String protocol;

  String targetDescription = "";

  public HttpTargetURI(String target) {
    this.target = target;

    int pos = target.indexOf("//");
    protocol = target.substring(0, pos - 1);
    int ipEndPos = target.indexOf(":", pos);
    String jdIP = target.substring(pos + 2, ipEndPos);
    int portEndPos = target.indexOf("/", ipEndPos);
    targetContext = target.substring(portEndPos + 1);
    targetPort = target.substring(ipEndPos + 1, portEndPos);

  }

  @Override
  public String getProtocol() {
    return protocol;
  }

  @Override
  public String getNodename() {
    return null;
  }

  @Override
  public String getPort() {
    return targetPort;
  }

  @Override
  public String getContext() {
    return targetContext;
  }

  @Override
  public String asString() {
    return target;
  }

  @Override
  public String getDescription() {
    return targetDescription;
  }

}
