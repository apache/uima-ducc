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
package org.apache.uima.ducc.ps.service;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.apache.uima.ducc.ph.Client;
import org.apache.uima.ducc.ps.service.builders.PullServiceStepBuilder;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.uima.UimaServiceProcessor;
import org.junit.Test;

public class JunitPullServiceTestSuite extends Client {

	CountDownLatch threadsReady;
	CountDownLatch stopLatch;

	@Test
	public void testPullService() throws Exception {
		int scaleout = 2;
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "TestAAE";
		IServiceProcessor processor = new 
				UimaServiceProcessor(analysisEngineDescriptor);

		String tasURL = "http://localhost:8080/test";
		
		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			service.initialize();
			Timer fTimer = new Timer("testPullService Timer");
			// after 5secs stop the pull service
			fTimer.schedule(new MyTimerTask(service, fTimer), 5000);
			
			service.start();

		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		}
	}
	@Test
	public void testPullServiceTimeout() throws Exception {
		super.startJetty(true);  // true=client blocks all POST requests
		int scaleout = 12;
		String analysisEngineDescriptor = "TestAAE";
		IServiceProcessor processor = new 
				UimaServiceProcessor(analysisEngineDescriptor);

		String tasURL ="http://localhost:8080/test";
		
		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			service.initialize();
			System.out.println("----------- Starting Service .....");
			Timer fTimer = new Timer();
			//after 10sec stop the service
			fTimer.schedule(new MyTimerTask(service, fTimer), 10000);

			service.start();

			
		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		}
	}
	/*
	@Test
	public void testPullServiceBadClientURL() throws Exception {
		int scaleout = 2;
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "TestAAE";
		IServiceProcessor processor = new 
				UimaServiceProcessor(analysisEngineDescriptor);

		String tasURL ="http://localhost2:8080/test";
		
		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			service.initialize();
			service.start();

			
		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		}
	}
	*/
	class MyTimerTask extends TimerTask {
		final IService service;
		final Timer fTimer;
		MyTimerTask(IService service, Timer fTimer) {
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
