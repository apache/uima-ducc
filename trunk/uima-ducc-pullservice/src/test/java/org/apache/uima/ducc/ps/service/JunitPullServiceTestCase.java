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

import org.apache.uima.ducc.ps.Client;
import org.apache.uima.ducc.ps.service.builders.PullServiceStepBuilder;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.uima.UimaServiceProcessor;
import org.junit.Test;

public class JunitPullServiceTestCase extends Client {
	private static final long  DELAY=5000;
	CountDownLatch threadsReady;
	CountDownLatch stopLatch;
	{
		// static initializer sets amount of time the service delays
		// sending READY to a monitor
		System.setProperty("ducc.service.init.delay", "3000");
	}
	@Test
	public void testPullService() throws Exception {
		System.out.println("----------------- testPullService -------------------");
		int scaleout = 2;
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "TestAAE";
		System.setProperty("ducc.deploy.JpType", "uima");

		IServiceProcessor processor = new
				UimaServiceProcessor(analysisEngineDescriptor);

		String tasURL = "http://localhost:"+super.getPort()+"/test";

		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			service.initialize();
			Timer fTimer = new Timer("testPullService Timer");
			// after 5secs stop the pull service
			fTimer.schedule(new MyTimerTask(service, fTimer, false), DELAY);

			service.start();

		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			super.stopJetty();
		}
	}
	@Test
	public void testPullServiceQuiesce() throws Exception {
		System.out.println("----------------- testPullServiceQuiesce -------------------");
		int scaleout = 2;
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "TestAAE";
		System.setProperty("ducc.deploy.JpType", "uima");
		IServiceProcessor processor = new
				UimaServiceProcessor(analysisEngineDescriptor);

		String tasURL = "http://localhost:"+super.getPort()+"/test";

		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			service.initialize();
			Timer fTimer = new Timer("testPullService Timer");
			// after 5secs stop the pull service
			fTimer.schedule(new MyTimerTask(service, fTimer, true), DELAY);

			service.start();

		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		}finally {
			super.stopJetty();
		}
	}

	@Test
	public void testPullServiceTimeout() throws Exception {
		System.out.println("----------------- testPullServiceTimeout -------------------");
		super.startJetty(true);  // true=client blocks all POST requests
		int scaleout = 12;
		String analysisEngineDescriptor = "TestAAE";
		IServiceProcessor processor = new
				UimaServiceProcessor(analysisEngineDescriptor);

		String tasURL ="http://localhost:"+super.getPort()+"/test";

		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			service.initialize();
			System.out.println("----------- Starting Service .....");
			Timer fTimer = new Timer();
			//after 10sec stop the service
			fTimer.schedule(new MyTimerTask(service, fTimer, false), DELAY);

			service.start();


		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		}finally {
			super.stopJetty();
		}
	}

	@Test
	public void testStopOnFirstError() throws Exception {
		System.out.println("----------------- testStopOnFirstError -------------------");
		int scaleout = 10;
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "NoOpAE";
		System.setProperty("ducc.deploy.JpType", "uima");

		IServiceProcessor processor =
				new UimaServiceProcessor(analysisEngineDescriptor);
		// fail on 1st error
		processor.setErrorHandlerWindow(1,  5);

		String tasURL = "http://localhost:"+super.getPort()+"/test";

		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			System.setProperty("ProcessFail","2");
			service.initialize();

			service.start();

		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			System.getProperties().remove("ProcessFail");
			super.stopJetty();
		}
	}
	@Test
	public void testTerminateOn2ErrorsInWindowOf5() throws Exception {
		System.out.println("----------------- testTerminateOn2ErrorsInWindowOf5 -------------------");
		int scaleout = 10;
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "NoOpAE";
		System.setProperty("ducc.deploy.JpType", "uima");

		IServiceProcessor processor =
				new UimaServiceProcessor(analysisEngineDescriptor);
		// fail on 2nd error in a window of 5
		processor.setErrorHandlerWindow(2,  5);
		String tasURL = "http://localhost:"+super.getPort()+"/test";

		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			// fail task#1 and task#3 which should stop the test
			System.setProperty("ProcessFail","1,3");
			service.initialize();

			service.start();

		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			System.getProperties().remove("ProcessFail");
			super.stopJetty();
		}
	}
	@Test
	public void testProcessFailureDefaultErrorHandler() throws Exception {
		System.out.println("----------------- testProcessFailureDefaultErrorHandler -------------------");
		int scaleout = 14;
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "NoOpAE";
		IServiceProcessor processor = new
				UimaServiceProcessor(analysisEngineDescriptor);

		String tasURL = "http://localhost:"+super.getPort()+"/test";

		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			// fail on 2nd task. This should terminate the test
			 System.setProperty("ProcessFail","20");
			service.initialize();
			Timer fTimer = new Timer("testPullService Timer");
			// after 5secs stop the pull service
			fTimer.schedule(new MyTimerTask(service, fTimer, false), 20000);

			service.start();

		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			System.getProperties().remove("ProcessFail");
			super.stopJetty();
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
		final boolean quiesce;

		MyTimerTask(IService service, Timer fTimer, boolean quiesce) {
			this.service = service;
			this.fTimer = fTimer;
			this.quiesce = quiesce;
		}

		@Override

		public void run() {
			this.cancel();
			fTimer.purge();
			fTimer.cancel();
			System.out.println("Timmer popped - stopping service");
			if (quiesce ) {
				service.quiesceAndStop();
			} else {
				service.stop();
			}
		}

	}

}
