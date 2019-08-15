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

/*
 * This represents the "Expose"d fields written by JED in the Experiment.state file
 * See com.ibm.bluej.system.driver/src/com/ibm/bluej/system/driver/TaskState.java 
 */
public class Task {
	  public int taskId = 0;
    public int parentId = 0;
    public String name = null;
    public String type = null;
    public String status = null;
    public String startTime = null;
    public long runTime = 0;
    public int[] subTasks = new int[0];
    public long[] duccId = new long[0];
}
