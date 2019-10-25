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
package org.apache.uima.ducc.ws;

import java.util.ArrayList;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.ws.handlers.experiments.HandlerExperimentsServlets;
import org.apache.uima.ducc.ws.server.DuccWebProperties;
import org.apache.uima.ducc.ws.server.DuccWebServer;
import org.apache.uima.ducc.ws.xd.ExperimentsRegistryManager;
import org.eclipse.jetty.server.Handler;

public class DuccPlugins {

  private static DuccLogger logger = DuccLogger.getLogger(DuccPlugins.class);

  private static DuccId jobid = null;

  private static DuccPlugins instance = new DuccPlugins();

  public static DuccPlugins getInstance() {
    return instance;
  }

  // Do nothing if not enabled
  private boolean experimentsEnabled;

  private ExperimentsRegistryManager experimentsRegistryManager = null;

  private DuccPlugins() {
    String ducc_experiments = DuccWebProperties.get().getProperty("ducc.experiments", "false");
    experimentsEnabled = ducc_experiments.equalsIgnoreCase("true");
    if (experimentsEnabled) {
      experimentsRegistryManager = ExperimentsRegistryManager.getInstance();
    }
  }
  
  /**
   * The restore methods are called during boot of the web server. This is an opportunity to have
   * local mods plug-in for processing that may be desirable relative to each Job, Reservation, and
   * Service during the restoration from history.
   * Note - exceptions are caught in DuccBoot
   */

  public void restore(IDuccWorkJob job) {
    if (experimentsRegistryManager != null) {
      experimentsRegistryManager.initialize(job);
    }
  }

  public void restore(IDuccWorkReservation reservation) {
    // Ignore unmanaged reservations
  }

  public void restore(IDuccWorkService service) {
    // Also process managed reservations in case the experiment has only these.
    // Note: APs are saved in DB as services of type "other"
    if (experimentsRegistryManager != null && service.getServiceDeploymentType() == ServiceDeploymentType.other) {
      experimentsRegistryManager.initialize(service);
    }
  }

  /**
   * The update method is called for each Orchestrator publication. This is an opportunity to have
   * local mods plug-in for processing that may be desirable relative to each Job, Reservation, and
   * Service within the published map.
   */

  public void update(IDuccWorkMap dwm) {
    String location = "update";
    try {
      if (experimentsRegistryManager != null) {
        experimentsRegistryManager.update(dwm);
      }
    } catch (Throwable t) {
      logger.error(location, jobid, t);
    }
  }

  /**
   * The gethandlers method is called during boot of the web server. This is an opportunity to have
   * local mods plug-in for processing that may be desirable relative to http/s requests.
   */

  public ArrayList<Handler> gethandlers(DuccWebServer duccWebServer) {
    String location = "gethandlers";
    ArrayList<Handler> handlersList = new ArrayList<Handler>();
    if (!experimentsEnabled) {
      return handlersList;
    }
    try {
      HandlerExperimentsServlets handlerExperimentsServlets = new HandlerExperimentsServlets(
              duccWebServer);
      handlersList.add(handlerExperimentsServlets);
    } catch (Throwable t) {
      logger.error(location, jobid, t);
    }
    return handlersList;
  }
}
