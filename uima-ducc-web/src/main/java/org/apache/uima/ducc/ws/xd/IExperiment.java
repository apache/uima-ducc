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
package org.apache.uima.ducc.ws.xd;

import java.util.ArrayList;

import org.apache.uima.ducc.common.utils.id.DuccId;

public interface IExperiment extends Comparable<IExperiment> {

  public String getUser();

  public String getDirectory();

  public ArrayList<Task> getTasks();

  public String getStartDate();

  public boolean isActive();

  public Jed.Status getStatus();

  public long getFileDate();

  public boolean isStale();

  /*
   *  Return the DuccId of the AP that launched the experiment, or null
   */
  public DuccId getJedDuccId();
  
  /*
   *  Update the DuccId of the AP that launched the experiment
   */
  public void updateJedId(DuccId duccId);
  
  /*
   *  Set or clear the Experiment restart status
   */
  public void updateStatus(String restartJedId);
  
  /*
   * Write the updated state file
   */
  public boolean writeStateFile(String umask);
}
