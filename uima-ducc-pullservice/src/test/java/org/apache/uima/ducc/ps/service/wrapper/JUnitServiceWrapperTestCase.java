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
package org.apache.uima.ducc.ps.service.wrapper;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.uima.ducc.ps.Client;
import org.apache.uima.ducc.ps.StateMonitor;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.main.ServiceWrapper;
import org.junit.Test;

public class JUnitServiceWrapperTestCase extends Client  {

	@Test
	public void testPullServiceWrapper() throws Exception {
		//int scaleout = 2;
		StateMonitor monitor = new StateMonitor();
		monitor.start();
		System.out.println("........... Monitor Port:"+System.getProperty("DUCC_STATE_UPDATE_PORT"));
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "TestAAE";

		String tasURL = "http://localhost:8080/test";
		try {
			System.setProperty("ducc.deploy.JdURL", tasURL);
			System.setProperty("ducc.deploy.JpThreadCount","4");
			System.setProperty("ducc.deploy.service.type", "NotesService");
			System.getProperty("ducc.deploy.JpType", "uima");

			ServiceWrapper service = new ServiceWrapper();

			Timer fTimer = new Timer("testPullService Timer");
			// after 5secs stop the pull service
			fTimer.schedule(new MyTimerTask(service, fTimer), 5000);
				
			service.initialize(new String[] {analysisEngineDescriptor});

			service.start();


		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			monitor.stop();

		}
	}
	
	@Test
	public void testPullServiceWrapperWithProcessFailure() throws Exception {
		//int scaleout = 2;
		StateMonitor monitor = new StateMonitor();
		monitor.start();
		System.out.println("........... Monitor Port:"+System.getProperty("DUCC_STATE_UPDATE_PORT"));
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "NoOpAE";

		String tasURL = "http://localhost:8080/test";
		try {
			// Force process failure
			System.setProperty("ProcessFail","true");
			 
			System.setProperty("ducc.deploy.JdURL", tasURL);
			System.setProperty("ducc.deploy.JpThreadCount","4");
			System.setProperty("ducc.deploy.service.type", "NotesService");
			System.getProperty("ducc.deploy.JpType", "uima");

			ServiceWrapper service = new ServiceWrapper();

			Timer fTimer = new Timer("testPullService Timer");
			// after 5secs stop the pull service
			fTimer.schedule(new MyTimerTask(service, fTimer), 5000);
				
			service.initialize(new String[] {analysisEngineDescriptor});

			service.start();


		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			monitor.stop();
			System.getProperties().remove("ProcessFail");
		}
	}
	class MyTimerTask extends TimerTask {
		final ServiceWrapper service;
		final Timer fTimer;
		MyTimerTask(ServiceWrapper service, Timer fTimer) {
			this.service = service;
			this.fTimer = fTimer;
		}
		
		        @Override
		
		        public void run() {
		        	this.cancel();
		        	fTimer.purge();
		        	fTimer.cancel();
		        	System.out.println("Timmer popped - stopping service");
		        	service.stop();
		        	
		        }
		
		 
		
		    }

}
