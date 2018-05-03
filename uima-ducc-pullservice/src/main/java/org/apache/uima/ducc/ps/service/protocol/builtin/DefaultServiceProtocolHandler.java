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
package org.apache.uima.ducc.ps.service.protocol.builtin;

import java.io.InvalidClassException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Type;
import org.apache.uima.ducc.ps.net.impl.MetaTaskTransaction;
import org.apache.uima.ducc.ps.net.impl.TransactionId;
import org.apache.uima.ducc.ps.service.IService;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler.Action;
import org.apache.uima.ducc.ps.service.errors.ServiceException;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.processor.IProcessResult;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.protocol.INoTaskAvailableStrategy;
import org.apache.uima.ducc.ps.service.protocol.IServiceProtocolHandler;
import org.apache.uima.ducc.ps.service.transport.IServiceTransport;
import org.apache.uima.ducc.ps.service.transport.TransportException;
import org.apache.uima.ducc.ps.service.transport.XStreamUtils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

/**
 * 
 * This protocol handler is a Runnable 
 *
 */
public class DefaultServiceProtocolHandler implements IServiceProtocolHandler {
	Logger logger = UIMAFramework.getLogger(DefaultServiceProtocolHandler.class);
	private volatile boolean initError = false;
	private volatile boolean running = false;
	private IServiceTransport transport;
	private IServiceProcessor processor;
	private INoTaskAvailableStrategy noTaskStrategy;
	// each process thread will count down the latch after intialization
	private CountDownLatch initLatch;
	// this PH will count the stopLatch down when it is about to stop. The service
	// is the owner of this latch and awaits termination blocking in start()
	private CountDownLatch stopLatch;
	// each process thread block on startLatch until application calls start()
	private CountDownLatch startLatch;
	// reference to a service so that stop() can be called
	private IService service;
	// forces process threads to initialize serially
	private ReentrantLock initLock = new ReentrantLock();
	
	private static AtomicInteger idGenerator = new AtomicInteger();

	
	private DefaultServiceProtocolHandler(Builder builder) { 
		this.initLatch = builder.initLatch; 
		this.stopLatch = builder.stopLatch; 
		this.service = builder.service;
		this.transport = builder.transport;
		this.processor = builder.processor;
		this.noTaskStrategy = builder.strategy;
	}

	private void waitForAllThreadsToInitialize() {
		try {
			initLatch.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

	}

	private void initialize() throws ServiceInitializationException {

		if (initError) {
			return;
		}
		// this latch blocks all process threads after initialization
		// until application calls start()
		startLatch = new CountDownLatch(1);
		try {
			// use a lock to serialize initialization one thread at a time
			initLock.lock();
			processor.initialize();
		} catch (Exception e) {
			initError = true;
			running = false;
			throw new ServiceInitializationException(
					"Thread:" + Thread.currentThread().getName() + " Failed initialization", e);
		} finally {

			initLatch.countDown();
			initLock.unlock();
			if (!initError) {
				// wait on startLatch
				waitForAllThreadsToInitialize();
			}
		}
	}

	private IMetaTaskTransaction send(IMetaTaskTransaction transaction) throws Exception {
		TransactionId tid;
		if (Type.Get.equals(transaction.getType())) {
			int major = idGenerator.addAndGet(1);
			int minor = 0;

			tid = new TransactionId(major, minor);
		} else {
			tid = transaction.getTransactionId();
			// increment minor
			tid.next();
		}
    	transaction.setRequesterProcessName(service.getType());
    	transport.addRequestorInfo(transaction);
		Object o = null;
		try {
			String body = XStreamUtils.marshall(transaction);
			String content = transport.dispatch(body);
			if ( content == null ) {
				throw new TransportException("Service stopping - rejecting request");
			}
			o = XStreamUtils.unmarshall(content);
			
		} catch ( Exception e) {
			if ( !running ) {
				System.out.println("... Not Running - throwing TransporException");
				throw new TransportException("Service stopping - rejecting request");
			}
			throw e;
		}
		if (o instanceof IMetaTaskTransaction) {
			return (MetaTaskTransaction) o;
		} else {
			throw new InvalidClassException(
					"Expected IMetaCasTransaction - Instead Received " + o.getClass().getName());
		}
	}

	private IMetaTaskTransaction callEnd(IMetaTaskTransaction transaction) throws Exception {
		transaction.setType(Type.End);
		if ( logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "ProtocolHandler calling END");
		}
		return send(transaction);

	}

	private IMetaTaskTransaction callAck(IMetaTaskTransaction transaction) throws Exception {
		transaction.setType(Type.Ack);
		if ( logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "ProtocolHandler calling ACK");
		}
		return send(transaction);
	}

	private IMetaTaskTransaction callGet(IMetaTaskTransaction transaction) throws Exception {
		transaction.setType(Type.Get); 
		if ( logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "ProtocolHandler calling GET");
		}
		return send(transaction);
	}
	/**
	 * Block until service start() is called
	 * 
	 * @throws ServiceInitializationException
	 */
	private void awaitStart() throws ServiceInitializationException {
		try {
			startLatch.await();
		} catch(InterruptedException e ) {
			Thread.currentThread().interrupt();
			throw new ServiceInitializationException("Thread interrupted while awaiting start()");
		}
	}
	public String call() throws ServiceInitializationException, ServiceException {
		// we may fail in initialize() in which case the ServiceInitializationException
		// is thrown
		initialize();
		
		// now wait for application to call start
		awaitStart();
		
		// all threads intialized, enter running state

		IMetaTaskTransaction transaction = null;
		
		if ( logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO, ".............. Thread "+Thread.currentThread().getId() + " ready to process");
		}

		
		while (running) {

			try {
				// send GET Request
				transaction = callGet(new MetaTaskTransaction());
				// the code may have blocked in callGet for awhile, so check
				// if service is still running
				if ( !running ) {
					break;
				}
				if (transaction.getMetaTask() == null || transaction.getMetaTask().getUserSpaceTask() == null ) {
					// the client has no tasks to give. 
					noTaskStrategy.handleNoTaskSupplied();
					continue;
				}
				Object task = transaction.getMetaTask().getUserSpaceTask();
				
				// send ACK 
				transaction = callAck(transaction);
				if (!running) {
					break;
				}
				IProcessResult processResult = processor.process((String) task);

				// assume success
				Action action = Action.CONTINUE;
				if (processResult.terminateProcess()) {
					action = Action.TERMINATE;
					String errorAsString = processResult.getError();
					IMetaTask mc = transaction.getMetaTask();
					mc.setUserSpaceException(errorAsString);
				} else {
					// success
					// System.out.println("Performance Metrics:"+processResult.getResult());
					transaction.getMetaTask().setPerformanceMetrics(processResult.getResult());
				}
				// send END Request
				callEnd(transaction);
				if (running && Action.TERMINATE.equals(action)) {
					logger.log(Level.WARNING, "Processor Failure - Action=Terminate");
					// Can't stop using the current thread. This thread
					// came from a thread pool we want to stop. Need
					// a new/independent thread to call stop()
					new Thread(new Runnable() {

						@Override
						public void run() {
							delegateStop();
						}
					}).start();
					running = false;
				}
					
				

			} catch( IllegalStateException e) {
				break;
			} catch( TransportException e) {
				break;
			}
			catch (Exception e) {
				logger.log(Level.WARNING,"",e);
			} 		
		}
		stopLatch.countDown();
		logger.log(Level.INFO,"ProtocolHandler terminated");
		return String.valueOf(Thread.currentThread().getId());
	}

	
	private void delegateStop() {
		service.stop();
	}
	@Override
	public void stop() {
		running = false;
		if ( logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO, this.getClass().getName()+" stop() called");
		}
	}
	@Override
	public void start() {
		running = true;
		// process threads are initialized and are awaiting latch countdown
		startLatch.countDown();
	}
	@Override
	public void setServiceProcessor(IServiceProcessor processor) {
		this.processor = processor;
	}

	@Override
	public void setTransport(IServiceTransport transport) {
		this.transport = transport;
	}
	
	
	 public static class Builder {
			private IServiceTransport transport;
			private IServiceProcessor processor;
			private INoTaskAvailableStrategy strategy;
			// each thread will count down the latch
			private CountDownLatch initLatch;
			private CountDownLatch stopLatch;
			private IService service;

			public Builder withTransport(IServiceTransport transport) {
				this.transport = transport;
				return this;
			}
			public Builder withProcessor(IServiceProcessor processor) {
				this.processor = processor;
				return this;
			}			
			public Builder withInitCompleteLatch(CountDownLatch initLatch) {
				this.initLatch = initLatch;
				return this;
			}			
			public Builder withDoneLatch(CountDownLatch stopLatch) {
				this.stopLatch = stopLatch;
				return this;
			}			
			public Builder withNoTaskStrategy(INoTaskAvailableStrategy strategy) {
				this.strategy = strategy;
				return this;
			}
			public Builder withService(IService service) {
				this.service = service;
				return this;
			}
			public DefaultServiceProtocolHandler build() {
	            return new DefaultServiceProtocolHandler(this);
	        }
	 }
}
