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

package org.apache.uima.ducc.transport.configurator.jp;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;
import org.apache.uima.ducc.container.net.iface.IPerformanceMetrics;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.container.net.impl.PerformanceMetrics;
import org.apache.uima.ducc.container.net.impl.TransactionId;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class HttpWorkerThread implements Runnable {
	DuccLogger logger = new DuccLogger(HttpWorkerThread.class);
	private DuccHttpClient httpClient = null;
	private JobProcessComponent duccComponent;
	private Object monitor = new Object();
	private CountDownLatch workerThreadCount = null;
	private CountDownLatch threadReadyCount = null;
	private Object processorInstance = null;
    private static AtomicInteger IdGenerator =
    		new AtomicInteger();
	public HttpWorkerThread(JobProcessComponent component, DuccHttpClient httpClient,
			Object processorInstance, CountDownLatch workerThreadCount,
			CountDownLatch threadReadyCount) {
		this.duccComponent = component;
		this.httpClient = httpClient;
		this.processorInstance = processorInstance;
		this.workerThreadCount = workerThreadCount;
		this.threadReadyCount = threadReadyCount;
	}
	@SuppressWarnings("unchecked")
	public void run() {
		String command="";
		PostMethod postMethod = null;
	    logger.info("HttpWorkerThread.run()", null, "Starting JP Process Thread Id:"+Thread.currentThread().getId());
	    Method processMethod = null;
	    boolean error=false;
	    // ***** DEPLOY ANALYTICS ***********
	    // First, deploy analytics in a provided process container. Use java reflection to call
	    // deploy method. The process container has been instantiated in the main thread and
	    // loaded from ducc-user jar provided in system classpath
	    try {
			processMethod = processorInstance.getClass().getDeclaredMethod("process", Object.class);	
			Method deployMethod = processorInstance.getClass().getDeclaredMethod("deploy", String.class);
			deployMethod.invoke(processorInstance, (Object)Utils.findDuccHome());

			// each thread needs its own PostMethod
			postMethod = new PostMethod(httpClient.getJdUrl());
			// Set request timeout
			postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, duccComponent.getTimeout());
	   	} catch( Throwable t) {
	   		error = true;
	   		synchronized(JobProcessComponent.class) {
				duccComponent.setState(ProcessState.FailedInitialization);
			}
            t.printStackTrace();
	   		logger.error("HttpWorkerThread.run()", null, t);
	   		System.out.println("EXITING WorkThread ID:"
					+ Thread.currentThread().getId());
	   		return;  // non-recovorable error
	   	} finally {
			// count down the latch. Once all threads deploy and initialize their analytics the processing
			// may being
			threadReadyCount.countDown();  // this thread is ready
			// **************************************************************************
			// now block and wait until all threads finish deploying and initializing 
			// analytics in provided process container. Processing begins when
			// all worker threads initialize their analytics.
			// **************************************************************************
			try {
				threadReadyCount.await();   // wait for all analytics to initialize
			} catch( Exception ie) {}

			if (!error) {
				synchronized(JobProcessComponent.class) {
					// change the state of this process and notify
					// Ducc agent that the process is ready and running
					duccComponent.setState(ProcessState.Running);
				}
			}
	   		
	   	}
			
			
	   	logger.info("HttpWorkerThread.run()", null, "Begin Processing Work Items - Thread Id:"+Thread.currentThread().getId());
		try {
			// Enter process loop. Stop this thread on the first process error.
			while (duccComponent.isRunning()) {  

				try {
					int major = IdGenerator.addAndGet(1);
					int minor = 0;

					IMetaCasTransaction transaction = new MetaCasTransaction();
					TransactionId tid = new TransactionId(major, minor);
					transaction.setTransactionId(tid);
					// According to HTTP spec, GET may not contain Body in 
					// HTTP request. HttpClient actually enforces this. So
					// do a POST instead of a GET.
					transaction.setType(Type.Get);  // Tell JD you want a Work Item
					command = Type.Get.name();
			    	logger.debug("HttpWorkerThread.run()", null, "Thread Id:"+Thread.currentThread().getId()+" Requesting next WI from JD");;
					// send a request to JD and wait for a reply
			    	transaction = httpClient.execute(transaction, postMethod);
                    // The JD may not provide a Work Item to process.
			    	if ( transaction.getMetaCas()!= null) {
    					logger.info("run", null,"Thread:"+Thread.currentThread().getId()+" Recv'd WI:"+transaction.getMetaCas().getSystemKey());
                    } else {
    					logger.debug("run", null,"Thread:"+Thread.currentThread().getId()+" Recv'd JD Response, however there is no MetaCas. Sleeping for "+duccComponent.getThreadSleepTime());
                    }

					// Confirm receipt of the CAS. 
					transaction.setType(Type.Ack);
					command = Type.Ack.name();
					tid = new TransactionId(major, minor++);
					transaction.setTransactionId(tid);
					httpClient.execute(transaction, postMethod); 
					
                    logger.debug("run", null,"Thread:"+Thread.currentThread().getId()+" Sent ACK");
                    
					// if the JD did not provide a Work Item, most likely the CR is
					// done. In such case, reduce frequency of Get requests
					// by sleeping in between Get's. Eventually the OR will 
					// deallocate this process and the thread will exit
					if ( transaction.getMetaCas() == null || transaction.getMetaCas().getUserSpaceCas() == null) {
						// the JD says there are no more WIs. Sleep awhile
						// do a GET in case JD changes its mind. The JP will
						// eventually be stopped by the agent
						synchronized (monitor) {
							try {
								// There is no CAS. It looks like the JD CR is done but there
								// are still WIs being processed. Slow down the rate of requests	
								monitor.wait(duccComponent.getThreadSleepTime());
							} catch (InterruptedException e) {
							}
						}
					} else {
						boolean workItemFailed = false;
						// process the Work item. Any exception here will cause the 
						// thread to terminate and also the JP to stop. The stopping
						// is orderly allowing each thread to finish processing of
						// the current WI. Once the JP notifies the Agent of a problem
						// the Agent will wait for 1 minute (default) before killing
						// this process via kill -9
						try {
							//    ********** PROCESS() **************
							// using java reflection, call process to analyze the CAS
							 List<Properties> metrics = (List<Properties>)processMethod.
							   invoke(processorInstance, transaction.getMetaCas().getUserSpaceCas());
							//    ***********************************
							 
		                    logger.debug("run", null,"Thread:"+Thread.currentThread().getId()+" process() completed");
							IPerformanceMetrics metricsWrapper =
									new PerformanceMetrics();
							metricsWrapper.set(metrics);
							
							transaction.getMetaCas().setPerformanceMetrics(metricsWrapper);
							
						}  catch( InvocationTargetException ee) {
							// The only way we would be here is if uimaProcessor.process() method failed.
							// In this case, the process method serialized stack trace into binary blob
							// and wrapped it in AnalysisEngineProcessException. The serialized stack 
							// trace is available via getMessage() call.

							// This is process error. It may contain user defined
							// exception in the stack trace. To protect against
						    // ClassNotFound, the entire stack trace was serialized.
							// Fetch the serialized stack trace and pass it on to
							// to the JD. The actual serialized stack trace is wrapped in
							// RuntimeException->AnalysisEngineException.message
							workItemFailed = true;
							IMetaCas mc = transaction.getMetaCas();
							
							// Fetch serialized exception as a blob
							Method getLastSerializedErrorMethod = processorInstance.getClass().getDeclaredMethod("getLastSerializedError");
							byte[] serializedException =
							    (byte[])getLastSerializedErrorMethod.invoke(processorInstance);
							mc.setUserSpaceException(serializedException);								

							logger.info("run", null, "Work item processing failed - returning serialized exception to the JD");
						} catch( Exception ee) {
							workItemFailed = true;
							// Serialize exception for the JD.
							ByteArrayOutputStream baos = new ByteArrayOutputStream();
						    ObjectOutputStream oos = new ObjectOutputStream( baos );
						    oos.writeObject( ee);
						    oos.close();
							transaction.getMetaCas().setUserSpaceException(baos.toByteArray());
							logger.error("run", null, ee);
						}
						// Dont return serialized CAS to reduce the msg size
						transaction.getMetaCas().setUserSpaceCas(null);
						transaction.setType(Type.End);
						command = Type.End.name();

						tid = new TransactionId(major, minor++);
						transaction.setTransactionId(tid);

						httpClient.execute(transaction, postMethod); // Work Item Processed - End
                    	String wid = null;
                    	try {
                    		wid = transaction.getMetaCas().getSystemKey();
                    	} catch( Exception e) {
                    		
                    	}
	                    logger.info("run", null,"Thread:"+Thread.currentThread().getId()+" sent END for WI:"+wid);
	                    if ( workItemFailed ) {
	                        if ( wid != null ) {
		                    	logger.warn("run", null, "Worker thread exiting due to error while processing WI:"+wid);
	                        } else {
		                    	logger.warn("run", null, "Worker thread exiting due to error while processing a WI");
	                        }
        					duccComponent.setState(ProcessState.Stopping);
                        	break;
                        }
					}
				} catch( SocketTimeoutException e) {
					logger.warn("run", null, "Timed Out While Awaiting Response from JD for "+command+" Request - Retrying ...");
					System.out.println("Time Out While Waiting For a Reply from JD For "+command+" Request");
				}
				catch (Exception e ) {
					logger.error("run", null, e);
					logger.error("run", null, "Caught Unexpected Exception - Exiting Thread "+Thread.currentThread().getId() );
					e.printStackTrace();
					break; 
				} finally {

				}

			}

		} catch (Throwable t) {
			t.printStackTrace();
			logger.error("run", null, t);
		} finally {
			logger.warn("run",null,"EXITING WorkThread ID:"
					+ Thread.currentThread().getId());
			System.out.println("EXITING WorkThread ID:"
					+ Thread.currentThread().getId());
		    try {
		    	// Determine if the Worker thread has thread affinity to specific AE
		    	// instance. This depends on the process container. If this process
		    	// uses pieces part (not DD), than the thread should call stop on
		    	// process container which will than destroy the AE. User code may
		    	// store stuff in ThreadLocal and use it in the destroy method.
		    	Method useThreadAffinityMethod = processorInstance.getClass().getDeclaredMethod("useThreadAffinity");	
				boolean useThreadAffinity =
						(Boolean)useThreadAffinityMethod.invoke(processorInstance);
				if ( useThreadAffinity) {
					Method stopMethod = processorInstance.getClass().getDeclaredMethod("stop");
					stopMethod.invoke(processorInstance);
				}
		   	} catch( Throwable t) {
		   		t.printStackTrace();
		   	} finally {
				workerThreadCount.countDown();
		   	}
		
		}

	}

}
