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
package org.apache.uima.ducc.ps.service.transport;

import java.util.HashMap;

import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.service.IServiceComponent;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;

import com.thoughtworks.xstream.XStream;

public interface IServiceTransport extends IServiceComponent {
  // called by Protocal Handler. Any errors will be handled
  // by instance of IServiceErrorHandler
  // public IMetaTaskTransaction dispatch(String request) throws TransportException;
  public IMetaTaskTransaction dispatch(String serializedRequest,
          ThreadLocal<HashMap<Long, XStream>> localXStream) throws TransportException;

  // initialize transport
  public void initialize() throws ServiceInitializationException;

  // stop transport
  public void stop(boolean quiesce);

  public void addRequestorInfo(IMetaTaskTransaction transaction);
}
