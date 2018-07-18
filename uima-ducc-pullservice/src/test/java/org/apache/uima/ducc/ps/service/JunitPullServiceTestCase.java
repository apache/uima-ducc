
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

	CountDownLatch threadsReady;
	CountDownLatch stopLatch;

	@Test
	public void testPullService() throws Exception {
		System.out.println("----------------- testPullService -------------------");
		int scaleout = 2;
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "TestAAE";
		System.setProperty("ducc.deploy.JpType", "uima");
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
			fTimer.schedule(new MyTimerTask(service, fTimer, false), 35000);
			
			service.start();

		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
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

		String tasURL = "http://localhost:8080/test";
		
		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			service.initialize();
			Timer fTimer = new Timer("testPullService Timer");
			// after 5secs stop the pull service
			fTimer.schedule(new MyTimerTask(service, fTimer, true), 35000);
			
			service.start();

		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
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

		String tasURL ="http://localhost:8080/test";
		
		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			service.initialize();
			System.out.println("----------- Starting Service .....");
			Timer fTimer = new Timer();
			//after 10sec stop the service
			fTimer.schedule(new MyTimerTask(service, fTimer, false), 40000);

			service.start();

			
		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		}
	}
	
	@Test
	public void testPullServiceWithProcessFailure() throws Exception {
		System.out.println("----------------- testPullServiceWithProcessFailure -------------------");
		int scaleout = 2;
		super.startJetty(false);  // don't block
		String analysisEngineDescriptor = "NoOpAE";
		IServiceProcessor processor = new 
				UimaServiceProcessor(analysisEngineDescriptor);

		String tasURL = "http://localhost:8080/test";
		
		IService service = PullServiceStepBuilder.newBuilder().withProcessor(processor)
				.withClientURL(tasURL).withType("Note Service").withScaleout(scaleout)
				.withOptionalsDone().build();

		try {
			 System.setProperty("ProcessFail","true");
			service.initialize();
			Timer fTimer = new Timer("testPullService Timer");
			// after 5secs stop the pull service
			fTimer.schedule(new MyTimerTask(service, fTimer, false), 35000);
			
			service.start();

		} catch (ServiceInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			System.getProperties().remove("ProcessFail");
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
