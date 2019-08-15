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
	
	private static ExperimentsRegistryManager experimentsRegistryManager = ExperimentsRegistryManager.getInstance();

	/**
	 * The restore methods are called during boot of the web server.
	 * This is an opportunity to have local mods plug-in for
	 * processing that may be desirable relative to each Job,
	 * Reservation, and Service during the restoration from history.
	 */
	
	public void restore(IDuccWorkJob job) {
		String location = "restore";
		try {
			if(job != null) {
				String user = job.getStandardInfo().getUser();
				String directory = job.getStandardInfo().getLogDirectory();
				logger.info(location, jobid, "user", user, "directory", directory);
				experimentsRegistryManager.initialize(user, directory);
			}
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
	}
	
	public void restore(IDuccWorkReservation reservation) {
		String location = "restore";
		try {
			//loc mods here
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
	}
	
	public void restore(IDuccWorkService service) {
		String location = "restore";
		try {
			// Also process managed reservations in case the experiment has only these.
		    if(service != null && service.getServiceDeploymentType() == ServiceDeploymentType.other) {
		        String user = service.getStandardInfo().getUser();
		        String directory = service.getStandardInfo().getLogDirectory();
		        experimentsRegistryManager.initialize(user, directory);
		    }
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
	}
	
	/**
	 * The update method is called for each Orchestrator publication.
	 * This is an opportunity to have local mods plug-in for
	 * processing that may be desirable relative to each Job,
	 * Reservation, and Service within the published map.
	 */
	
	public void update(IDuccWorkMap dwm) {
		String location = "update";
		try {
			experimentsRegistryManager.update(dwm);
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
	}
	
	/**
	 * The gethandlers method is called during boot of the web server.
	 * This is an opportunity to have local mods plug-in for
	 * processing that may be desirable relative to http/s requests.
	 */
	
	public ArrayList<Handler> gethandlers(DuccWebServer duccWebServer) {
		String location = "gethandlers";
		ArrayList<Handler> handlersList = new ArrayList<Handler> ();
		try {
			HandlerExperimentsServlets handlerExperimentsServlets = new HandlerExperimentsServlets(duccWebServer);
			handlersList.add(handlerExperimentsServlets);
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
		return handlersList;
	}
}
