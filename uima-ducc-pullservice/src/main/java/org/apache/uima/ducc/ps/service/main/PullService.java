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
package org.apache.uima.ducc.ps.service.main;


import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.ServiceThreadFactory;
import org.apache.uima.ducc.ps.service.IService;
//import org.apache.uima.ducc.ps.service.ServiceConfiguration;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler;
import org.apache.uima.ducc.ps.service.errors.ServiceException;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.monitor.IServiceMonitor;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.protocol.IServiceProtocolHandler;
import org.apache.uima.ducc.ps.service.protocol.builtin.DefaultNoTaskAvailableStrategy;
import org.apache.uima.ducc.ps.service.protocol.builtin.DefaultServiceProtocolHandler;
import org.apache.uima.ducc.ps.service.registry.DefaultRegistryClient;
import org.apache.uima.ducc.ps.service.registry.IRegistryClient;
import org.apache.uima.ducc.ps.service.transport.IServiceTransport;
import org.apache.uima.ducc.ps.service.transport.ITargetURI;
import org.apache.uima.ducc.ps.service.transport.http.HttpServiceTransport;
import org.apache.uima.ducc.ps.service.transport.target.TargetURIFactory;
import org.apache.uima.ducc.ps.service.utils.Utils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class PullService implements IService {
	Logger logger = UIMAFramework.getLogger(PullService.class);
	// provide processing threads
	private ScheduledThreadPoolExecutor threadPool ;
	// how many processing threads
	private int scaleout=1;
	// amount of time to wait when client has no tasks to give
	private int waitTimeInMillis=0;  
	
	// application assigned service label
	private String type;
	private volatile boolean initialized = false;
	// ******************************************
	// application must plugin IRegistryClient instance or
	// specify clientURL to use. It's an error if neither
	// is provided
	private String clientURL;
	private IRegistryClient registryClient;
	// ******************************************

	// internal error handler
	private IServiceErrorHandler errorHandler=null;
	//
	private IServiceMonitor serviceMonitor=null;
	// internal transport to communicate with remote client
	private IServiceTransport transport=null;
	// internal protocol handler
	private IServiceProtocolHandler protocolHandler=null;
	// application provided service processor
	private IServiceProcessor serviceProcessor;
	// counts down when thread completes initialization or fails
	// while initializing
	private CountDownLatch threadsReady;
	// holds Future to every process thread
	private List<Future<String>> threadHandleList =
			new ArrayList<>();

	private Lock initLock = new ReentrantLock();

	private Application application=null;
	
	
	public PullService(String type) {
		this(type,null);

	}

	public PullService(String type, Application  application ) {
		this.type = type;

		this.application = application;
	}
	
	public String getType() {
		return type;
	}
	public void setWaitTime(int waitTimeInMillis) {
		this.waitTimeInMillis = waitTimeInMillis;
	}
	public void setScaleout(int scaleout) {
		this.scaleout = scaleout;
		this.threadsReady = new CountDownLatch(scaleout);
	}
	private void setErrorHandler(IServiceErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}
	private void setMonitor(IServiceMonitor monitor) {
		this.serviceMonitor = monitor;
	}
	private void setProtocolHandler(IServiceProtocolHandler protocolHandler) {
		this.protocolHandler = protocolHandler;
	}
	private void setTransport(IServiceTransport transport) {
		this.transport = transport;
	}
	public void setServiceProcessor(IServiceProcessor serviceProcessor) {
		this.serviceProcessor = serviceProcessor;
	}

	public void setRegistryClient(IRegistryClient registryClient) {
		this.registryClient = registryClient;
	}
	public void setClientURL(String clientURL) {
		this.clientURL = clientURL;
	}
	private void initializeDefaultRegistryClient() throws ServiceInitializationException {
		ITargetURI target;
		if (clientURL == null || clientURL.isEmpty()) {
			throw new ServiceInitializationException(
					"Application must plugin IRegistryClient instance or provide a valid client URL");
		}
		try {
			target = TargetURIFactory.newTarget(clientURL);
		} catch (ServiceException e) {
			throw new ServiceInitializationException("Unsupported registry URL " + clientURL, e);
		}
		registryClient = new DefaultRegistryClient(target);

	}

	@Override
	public void initialize() throws ServiceInitializationException {
		// only one thread can call this method
		initLock.lock();

		try {
			if ( initialized ) {
				// Already initialized
				return;
			}
			// if application does not plug in IRegistruClient instance use a default
			// builtin registry which requires application provided client URL
			if (registryClient == null) {
				// the following will throw exception if client URL not specified
				initializeDefaultRegistryClient();
			}

			// add default transport
			transport = new HttpServiceTransport(registryClient, scaleout);

			// contract is that the service will block in this method until
			// all process threads initialize. Use a latch to block until this
			// happens. Each process thread will count this down after initialization
			if ( threadsReady == null ) {
				this.threadsReady = new CountDownLatch(scaleout);
			}
			// contract is that the service will block in start() until application
			// calls stop() or there is a fatal error. Each process thread will count
			// this down just before thread dies.
			CountDownLatch stopLatch = new CountDownLatch(scaleout);
			serviceProcessor.setScaleout(scaleout);
			// add default protocol handler
	        protocolHandler =
					   new DefaultServiceProtocolHandler.Builder()
					   .withProcessor(serviceProcessor)
					   .withNoTaskStrategy(new DefaultNoTaskAvailableStrategy(waitTimeInMillis))
					   .withService(this)
					   .withTransport(transport)
					   .withDoneLatch(stopLatch)
					   .withInitCompleteLatch(threadsReady)
					   .build();


			// first initialize Processors. The ServiceThreadFactory creates
			// as many threads as defined in 'scaleout'
			threadPool =
					new ScheduledThreadPoolExecutor(scaleout, new ServiceThreadFactory());

	    	// Create and start worker threads that pull Work Items from a client.
			// Each worker thread calls processor.initialize() and counts down the
			// 'threadsReady' latch. When all threads finish initializing they all
			// block until application calls IService.start()
			for (int j = 0; j < scaleout; j++) {
				threadHandleList.add( threadPool.submit(protocolHandler));
			}
			// wait until all process threads initialize
			threadsReady.await();

			initializeMonitor();
			initializeTransport();

			initialized = true;


		} catch( ServiceInitializationException e) {
			throw e;
		} catch( InterruptedException e) {
			Thread.currentThread().interrupt();
			threadPool.shutdownNow();
			throw new ServiceInitializationException("Service interrupted during initialization - shutting down process threads");
		} catch( Exception e) {
			throw new ServiceInitializationException("",e);
		}
		finally {
			initLock.unlock();
		}

	}

	@Override
	public void start() throws IllegalStateException, ExecutionException, ServiceException {
		if ( !initialized ) {
			throw new IllegalStateException("Application must call initialize() before calling start()");
		}
		try {
			// unblock process threads to begin fetching and processing
			// tasks.
			protocolHandler.start();
			// wait until all process threads terminate
			waitForProcessThreads();

		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			if ( threadPool.isTerminating() ) {
				return;
			} else {
				// thread has been interrupted, force executor shutdown
				threadPool.shutdownNow();
			}
		} catch( ExecutionException | ServiceException e) {
			logger.log(Level.WARNING,"",e);
			throw e;
		} catch( Throwable t) {
			logger.log(Level.WARNING,"",t);
			logger.log(Level.WARNING,"","Service is terminating due to failure to start");
			stop();
		}
	}
	@Override
	public void stop() {
		// process threads should stop first to avoid trying to pull new
		// work while threads are running
		//stopProcessThreadPool();
		logger.log(Level.INFO, "Stopping Process Thread Pool");
		threadPool.shutdownNow();
		// close connection to remote client and cleanup
		stopTransport();
		System.out.println(">>>>>>>> "+Utils.getTimestamp()+" "+Utils.getShortClassname(this.getClass())+" .quiesceAndStop()-transport stopped");
		stopProtocolHandler(false);
		System.out.println(">>>>>>>> "+Utils.getTimestamp()+" "+Utils.getShortClassname(this.getClass())+" .quiesceAndStop()-protocol handler stopped");
		stopServiceProcessor();
		System.out.println(">>>>>>>> "+Utils.getTimestamp()+" "+Utils.getShortClassname(this.getClass())+" .stop()-processor stopped");
	    // monitor should be stopped last to keep posting updates to observer
		stopMonitor();
		System.out.println(">>>>>>>> "+Utils.getTimestamp()+" "+Utils.getShortClassname(this.getClass())+" .dtop()-monitor stopped");
	}
    public void quiesceAndStop() {
		// when quiescing, let the process threads finish processing 
    	stopProtocolHandler(true);  // true = quiesce
		System.out.println(">>>>>>>> "+Utils.getTimestamp()+" "+Utils.getShortClassname(this.getClass())+" .quiesceAndStop()-protocol handler stopped");
		// close connection to remote client and cleanup
		stopTransport();
		System.out.println(">>>>>>>> "+Utils.getTimestamp()+" "+Utils.getShortClassname(this.getClass())+" .quiesceAndStop()-transport stopped");
		stopServiceProcessor();
		System.out.println(">>>>>>>> "+Utils.getTimestamp()+" "+Utils.getShortClassname(this.getClass())+" .quiesceAndStop()-processor stopped");
        // monitor should be stopped last to keep posting updates to observer
		stopMonitor();
		System.out.println(">>>>>>>> "+Utils.getTimestamp()+" "+Utils.getShortClassname(this.getClass())+" .quiesceAndStop()-monitor stopped");
    }
	private void waitForProcessThreads() throws InterruptedException, ExecutionException {
		for (Future<String> future : threadHandleList) {
			// print the return value of Future, notice the output delay in console
			// because Future.get() waits for task to get completed
			String result = future.get();
			logger.log(Level.INFO, "Thread:" + Thread.currentThread().getName() + " Terminated " + new Date() + "::" + result);
		}
		stopProcessThreadPool();
		if ( Objects.nonNull(application) ) {
			application.onServiceStop();
		}
	}

	
	private void initializeTransport() throws ServiceInitializationException {
		try {
			transport.initialize();
		} catch( Exception cause) {
			throw new ServiceInitializationException("Service Unable to Initialize Transport", cause);
		}
	}

	private void initializeMonitor() throws ServiceInitializationException {
		if ( serviceMonitor != null ) {
			try {
				serviceMonitor.initialize();
			} catch( Exception cause) {
				throw new ServiceInitializationException("Service Unable to Initialize Monitor", cause);
			}
		}
	}

	private void stopProcessThreadPool() {
//		if (threadPool != null && !threadPool.isShutdown() && !threadPool.isTerminating() && !threadPool.isTerminated()) {
		if (threadPool != null ) {

			try {
				logger.log(Level.INFO, "Stopping Process Thread Pool");
				threadPool.shutdownNow();
				
				// below probably not needed since this is done in start()
				threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
				logger.log(Level.INFO, "Process Thread Pool Stopped");

			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

	}


	private void stopMonitor() {
		if ( serviceMonitor != null ) {
			serviceMonitor.stop();
		}
	}
	private void stopServiceProcessor() {
		if ( serviceProcessor != null ) {
			serviceProcessor.stop();
		}
	}
	private void stopProtocolHandler(boolean quiesce) {
		if ( quiesce ) {
			protocolHandler.quiesceAndStop();
		} else {
			protocolHandler.stop();
		}
	}
	private void stopTransport() {
		transport.stop(false);   // !quiesce
	}
	public static void main(String[] args) {

	}


}
