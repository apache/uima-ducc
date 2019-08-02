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
package org.apache.uima.ducc.ps.net.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Hint;

public class TransactionHelper {

  // private static Logger logger = Logger.getLogger(TransactionHelper.class,
  // IComponent.Id.JD.name());

  public static void addResponseHint(IMetaTaskTransaction trans, Hint hint) {
    String location = "addResponseHint";
    if (trans != null) {
      if (hint != null) {
        List<Hint> hints = trans.getResponseHints();
        if (hints == null) {
          hints = new ArrayList<Hint>();
          trans.setResponseHints(hints);
        }
        if (!hints.contains(hint)) {
          hints.add(hint);
          // IRemoteWorkerProcess rwp = new RemoteWorkerProcess(trans);
          // MessageBuffer mb = new MessageBuffer();
          // mb.append(Standardize.Label.node.get()+rwp.getNodeName());
          // mb.append(Standardize.Label.pid.get()+rwp.getPid());
          // mb.append(Standardize.Label.value.get()+hint);
          // logger.debug(location, ILogger.null_id, mb.toString());
        }
      }
    }
  }
}
