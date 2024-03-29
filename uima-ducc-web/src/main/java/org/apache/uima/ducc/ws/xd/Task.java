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

import com.google.gson.annotations.Expose;

/*
 * The "exposed" fields are those exposed and saved by JED in the Experiment.state file
 * See TaskState in the com.ibm.watsonx.framework.jed project 
 */
public class Task implements Comparable<Task> {
  
  @Expose
  public String pathId;
  
  @Expose
  public int taskId;

  @Expose
  public int parentId;
  
  @Expose
  public String name;

  @Expose
  public String type;

  @Expose
  public String status;

  @Expose
  public String startTime;

  @Expose
  public long runTime;

  @Expose
  public int[] subTasks;

  @Expose
  public long[] duccId;
  
  public boolean rerun = false;  // Note - this is NOT in the Experiment.state file
  
  @Override
  public int compareTo(Task that) {
    return this.taskId - that.taskId;   // So can sort tasks for display
  }
}
