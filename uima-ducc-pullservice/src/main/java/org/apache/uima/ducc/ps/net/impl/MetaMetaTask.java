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

import org.apache.uima.ducc.ps.net.iface.IMetaMetaTask;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;

/**
 * An implementation for storing MetaCAS and CasManager status together
 */

public class MetaMetaTask implements IMetaMetaTask {

  private boolean exhausted;

  private boolean premature;

  private boolean killJob;

  private IMetaTask metaCas;

  public MetaMetaTask() {
  }

  @Override
  public boolean isExhausted() {
    return exhausted;
  }

  @Override
  public void setExhausted(boolean value) {
    exhausted = value;
  }

  @Override
  public boolean isPremature() {
    return premature;
  }

  @Override
  public void setPremature(boolean value) {
    premature = value;
  }

  @Override
  public boolean isKillJob() {
    return killJob;
  }

  @Override
  public void setKillJob(boolean value) {
    killJob = value;
  }

  @Override
  public IMetaTask getMetaCas() {
    return metaCas;
  }

  @Override
  public void setMetaCas(IMetaTask value) {
    metaCas = value;
  }

}
