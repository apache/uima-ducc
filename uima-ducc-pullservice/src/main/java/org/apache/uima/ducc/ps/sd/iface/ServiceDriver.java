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

package org.apache.uima.ducc.ps.sd.iface;

import java.util.Properties;

import org.apache.uima.ducc.ps.sd.DriverException;
import org.apache.uima.ducc.ps.sd.task.iface.TaskAllocatorCallbackListener;

public interface ServiceDriver extends Lifecycle {
  public static String Application = "driver.application.name";

  public static String Port = "driver.server.port";

  public static String MaxThreads = "driver.server.max.threads";

  public static String DriverTaskRetryCount = "driver.task.retry.count";

  public static String DriverTaskWaitTime = "driver.task.wait.time.millis";

  public String initialize(Properties props) throws DriverException;

  public TaskAllocatorCallbackListener getTaskAllocator();

  public void setTaskAllocator(TaskAllocatorCallbackListener taskAllocator);
}
