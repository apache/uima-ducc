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
package org.apache.uima.ducc.agent.deploy;

import java.util.List;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.transport.agent.IUimaPipelineAEComponent;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public abstract class AbstractManagedService extends AbstractDuccComponent
        implements ManagedService {
  public static final String ManagedServiceNotificationInterval = "uima.process.notify.interval";

  private long notificationInterval = 5000;

  protected ProcessState currentState = ProcessState.Undefined;

  protected ProcessState previousState = ProcessState.Undefined;

  // public static ManagedServiceContext serviceContext=null;
  public boolean useJmx = false;

  public ServiceStateNotificationAdapter serviceAdapter = null;

  public abstract void quiesceAndStop();

  public abstract void deploy(String[] args) throws Exception;

  protected AbstractManagedService(ServiceStateNotificationAdapter serviceAdapter,
          CamelContext context) {
    super("UimaProcess", context);
    this.serviceAdapter = serviceAdapter;
    // serviceContext = new ManagedServiceContext(this);
  }

  /**
   * @return the notificationInterval
   */
  public long getNotificationInterval() {
    return notificationInterval;
  }

  /**
   * @param notificationInterval
   *          the notificationInterval to set
   */
  public void setNotificationInterval(long notificationInterval) {
    this.notificationInterval = notificationInterval;
  }

  public void initialize() throws Exception {

    // ServiceShutdownHook shutdownHook = new ServiceShutdownHook(this);
    // serviceDeployer);
    // Runtime.getRuntime().addShutdownHook(shutdownHook);
    // System.out.println("Managed Service Wrapper Registered Shutdown Hook");
  }

  public void notifyAgentWithStatus(ProcessState state) {
    serviceAdapter.notifyAgentWithStatus(state);
  }

  public void notifyAgentWithStatus(ProcessState state, String message) {
    serviceAdapter.notifyAgentWithStatus(state, message);
  }

  public void notifyAgentWithStatus(List<IUimaPipelineAEComponent> pipeline) {
    serviceAdapter.notifyAgentWithStatus(pipeline);
  }

  protected void stopIt() {
    if (serviceAdapter != null) {
      // serviceAdapter.stop();
    }
  }

  /**
   * Returns state of this process( INITIALIZING, RUNNING, FAILED, STOPPED )
   */
  public ProcessState getServiceState() {
    return currentState;
  }

  @Override
  public void start(DuccService service, String[] args) throws Exception {
    try {
      super.start(service, args);
      deploy(args);
    } catch (Exception e) {
      currentState = ProcessState.FailedInitialization;
      notifyAgentWithStatus(ProcessState.FailedInitialization);
      throw e;
    }
  }

  public void stop() {
    if (super.isStopping()) {
      return; // already stopping - nothing to do
    }
    try {
      System.out.println("... AbstractManagedService - Stopping Service Adapter");
      serviceAdapter.stop();
      System.out.println("... AbstractManagedService - Calling super.stop() ");
      super.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static class ServiceShutdownHook extends Thread {
    private AbstractManagedService managedService;

    public ServiceShutdownHook(AbstractManagedService service) {
      this.managedService = service;
    }

    public void run() {
      try {
        System.out.println(
                "Uima AS Service Wrapper Caught Kill Signal - Initiating Quiesce and Stop");
        managedService.quiesceAndStop();
        managedService.stopIt();

      } catch (Exception e) {
      }
    }
  }

}
