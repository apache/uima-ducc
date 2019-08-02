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

package org.apache.uima.ducc.ps.service.dgen;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.ps.service.ServiceConfiguration;
import org.apache.uima.ducc.ps.service.dgen.iface.IDuccGeneratorUimaAggregate;
import org.apache.uima.ducc.ps.service.dgen.iface.IDuccGeneratorUimaAggregateComponent;

public class DuccUimaAggregate implements IDuccGeneratorUimaAggregate {

  /**
   * please increment this sUID when removing or modifying a field
   */
  private static final long serialVersionUID = 1L;

  private List<IDuccGeneratorUimaAggregateComponent> components = new ArrayList<IDuccGeneratorUimaAggregateComponent>();

  private int threadCount = 1;

  private String flowController;

  public DuccUimaAggregate(ServiceConfiguration serviceConfiguration,
          List<IDuccGeneratorUimaAggregateComponent> components) {
    if (serviceConfiguration.getThreadCount() == null
            || serviceConfiguration.getThreadCount().trim().isEmpty()) {
      setThreadCount(1);
    } else {
      setThreadCount(Integer.valueOf(serviceConfiguration.getThreadCount()));
    }
    setFlowController(serviceConfiguration.getJpFlowController());
    setComponents(components);
  }

  public List<IDuccGeneratorUimaAggregateComponent> getComponents() {
    return components;
  }

  public void setComponents(List<IDuccGeneratorUimaAggregateComponent> components) {
    this.components = components;
  }

  public int getThreadCount() {
    return threadCount;
  }

  public void setThreadCount(int threadCount) {
    this.threadCount = threadCount;
  }

  public String getFlowController() {
    return flowController;
  }

  public void setFlowController(String flowController) {
    this.flowController = flowController;
  }
}