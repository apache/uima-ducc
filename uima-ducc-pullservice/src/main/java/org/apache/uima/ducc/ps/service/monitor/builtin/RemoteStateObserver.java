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
package org.apache.uima.ducc.ps.service.monitor.builtin;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

import org.apache.uima.ducc.ps.service.IServiceState;
import org.apache.uima.ducc.ps.service.ServiceConfiguration;
import org.apache.uima.ducc.ps.service.monitor.IServiceMonitor;
import org.apache.uima.ducc.ps.service.transport.XStreamUtils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class RemoteStateObserver implements IServiceMonitor {
	private static final String SERVICE_JMS_PORT = "SERVICE_JMX_PORT=";
	private static final String SERVICE_UNIQUE_ID= "DUCC_PROCESS_UNIQUEID=";
	private static final String SERVICE_STATE = "DUCC_PROCESS_STATE=";
	private static final String SERVICE_DATA = "SERVICE_DATA=";
	private static final String SEPARATOR = ",";
	private ServiceConfiguration serviceConfiguration;
	private Logger logger;
	private String currentState = IServiceState.State.Starting.toString();
	public RemoteStateObserver(ServiceConfiguration serviceConfiguration, Logger logger) {
		this.serviceConfiguration = serviceConfiguration;
		this.logger = logger;
		Properties serviceProps = new Properties();

		sendStateUpdate(currentState, serviceProps);
	}

	private Socket connect() throws IOException {
		int statusUpdatePort = -1;

		String port = serviceConfiguration.getMonitorPort();
		try {
			statusUpdatePort = Integer.valueOf(port);
		} catch (NumberFormatException nfe) {
			return null; 
		}
	    logger.log(Level.INFO, "Service Connecting Socket to localhost Monitor on port:" + statusUpdatePort);
		String localhost = null;
		// establish socket connection to an agent where this process will report its
		// state
		return new Socket(localhost, statusUpdatePort);

	}

	private void sendStateUpdate(String state, Properties additionalData){
		DataOutputStream out = null;
		Socket socket = null;
		// if this process is not launched by an agent, the update port will be missing
		// Dont send updates.
		if (serviceConfiguration.getMonitorPort() == null || serviceConfiguration.getDuccProcessUniqueId() == null) {
			return;
		}
		try {
			socket = connect();
			if ( socket == null ) {
				return;
			}
			String serviceData = "";
			if ( additionalData == null ) {
				additionalData = new Properties();
			} 
			if ( serviceConfiguration.getAssignedJmxPort() != null && 
					!serviceConfiguration.getAssignedJmxPort().trim().isEmpty()) {
				additionalData.setProperty(SERVICE_JMS_PORT, serviceConfiguration.getAssignedJmxPort().trim());
			}
			serviceData = XStreamUtils.marshall(additionalData);
			
			StringBuilder sb = new StringBuilder()
			   .append(SERVICE_UNIQUE_ID)
			   .append(serviceConfiguration.getDuccProcessUniqueId())
			   .append(SEPARATOR)
			   .append(SERVICE_STATE)
			   .append(state)
			   .append(SEPARATOR)
			   .append(SERVICE_DATA)
			   .append(serviceData);
			out = new DataOutputStream(socket.getOutputStream());
			out.writeUTF(sb.toString());
			out.flush();
//			if (logger.isLoggable(Level.FINE)) {
//				logger.log(Level.FINE, "Sent new State:" + state);
//			}
		} catch (Exception e) {
			
		} finally {
			try {
				if (out != null) {
					out.close();
				}
				if (socket != null) {
					socket.close();
				}
			} catch( IOException ee) {
				
			}
		
		}

	}

	@Override
	public void initialize() {
		
	}

	@Override
	public void onStateChange(String state, Properties additionalData) {
		sendStateUpdate(state, additionalData);
	}
	@Override
	public void onStateChange(Properties additionalData) {
		sendStateUpdate(currentState, additionalData);
	}

	@Override
	public void stop() {
		
	}
	public static void main(String[] args) {

	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}
}
