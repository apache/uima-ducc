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

package org.apache.uima.ducc.transport.configuration.jp;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.methods.HttpPost;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Type;
import org.apache.uima.ducc.ps.net.impl.MetaTaskTransaction;

import org.apache.uima.ducc.ps.net.impl.TransactionId;
import org.apache.uima.ducc.ps.service.processor.IServiceResultSerializer;
import org.apache.uima.ducc.ps.service.processor.uima.utils.PerformanceMetrics;
import org.apache.uima.ducc.ps.service.processor.uima.utils.UimaResultDefaultSerializer;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
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
    private Map<String, IMetaTaskTransaction> transactionMap =
    		new ConcurrentHashMap<String, IMetaTaskTransaction>();
    static AtomicInteger maxFrameworkFailures;
    private int maxFrameworkErrors = 2;   // default
    // define what happens to this jvm when process() method fails.
    // The default is to call exit() but user may override this and
    // keep on running.
    private final boolean exitOnProcessFailure;
    
	public HttpWorkerThread(JobProcessComponent component, DuccHttpClient httpClient,
			Object processorInstance, CountDownLatch workerThreadCount,
			CountDownLatch threadReadyCount, Map<String, IMetaTaskTransaction> transactionMap,
			AtomicInteger maxFrameworkFailures ) {
		this.duccComponent = component;
		this.httpClient = httpClient;
		this.processorInstance = processorInstance;
		this.workerThreadCount = workerThreadCount;
		this.threadReadyCount = threadReadyCount;
		this.transactionMap = transactionMap;
		HttpWorkerThread.maxFrameworkFailures = maxFrameworkFailures;
		maxFrameworkErrors = maxFrameworkFailures.get();
		String exitProperty = System.getProperty("ExitOnProcessFailure");
		if ( exitProperty == null || exitProperty.trim().equalsIgnoreCase("true")) {
			exitOnProcessFailure = true;
		} else  {
			if ( exitProperty.trim().equalsIgnoreCase("false") ) {
				exitOnProcessFailure = false;
			} else {
				throw new IllegalArgumentException("Invalid value for property ExitOnProcessFailure. Should be [true/false] but is "+exitProperty);
			}
		}
	}   

	public IMetaTaskTransaction getWork(HttpPost postMethod, int major, int minor) throws Exception {
		String command="";

		IMetaTaskTransaction transaction = new MetaTaskTransaction();
		try {
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
	    	//httpClient.testConnection();
	        // The JD may not provide a Work Item to process.
	    	if ( transaction != null && transaction.getMetaTask()!= null) {
				logger.info("run", null,"Thread:"+Thread.currentThread().getId()+" Recv'd WI:"+transaction.getMetaTask().getSystemKey());
				// Confirm receipt of the CAS. 
				transaction.setType(Type.Ack);
				command = Type.Ack.name();
				tid = new TransactionId(major, minor++);
				transaction.setTransactionId(tid);
				logger.debug("run", null,"Thread:"+Thread.currentThread().getId()+" Sending ACK request - WI:"+transaction.getMetaTask().getSystemKey());
				transaction = httpClient.execute(transaction, postMethod); 
				if ( transaction.getMetaTask() == null) {
					// this can be the case when a JD receives ACK late 
					logger.info("run", null,"Thread:"+Thread.currentThread().getId()+" ACK reply recv'd, however there is no MetaCas. The JD Cancelled the transaction");
				} else {
		            logger.debug("run", null,"Thread:"+Thread.currentThread().getId()+" ACK reply recv'd");
				}

	        }
		} catch( SocketTimeoutException e) {
			logger.warn("run", null, "Timed Out While Awaiting Response from JD for "+command+" Request - Retrying ...");
		} 
    	return transaction;

	}

	private void waitAwhile(long sleepTime) throws InterruptedException {
		synchronized (monitor) {
			// There is no CAS. It looks like the JD CR is done but there
			// are still WIs being processed. Slow down the rate of requests	
			monitor.wait(sleepTime);
		}
	}
	@SuppressWarnings("unchecked")
	public void run() {
		// when this thread looses connection to its JD, log error once
		boolean logConnectionToJD = true;
		HttpPost postMethod = null;
	    logger.info("HttpWorkerThread.run()", null, "Starting JP Process Thread Id:"+Thread.currentThread().getId());
	    Method processMethod = null;
	    Method getKeyMethod = null;
	    boolean error=false;
	    // ***** DEPLOY ANALYTICS ***********
	    // First, deploy analytics in a provided process container. Use java reflection to call
	    // deploy method. The process container has been instantiated in the main thread and
	    // loaded from ducc-user j      ar provided in system classpath
	    try {
			processMethod = processorInstance.getClass().getSuperclass().getDeclaredMethod("process", Object.class);	
			getKeyMethod = processorInstance.getClass().getSuperclass().getDeclaredMethod("getKey", String.class);	
			
			synchronized(HttpWorkerThread.class) {
				Method deployMethod = processorInstance.getClass().getSuperclass().getDeclaredMethod("deploy");
				deployMethod.invoke(processorInstance);
				logger.info("HttpWorkerThread.run()", null,".... Deployed Processing Container - Initialization Successful - Thread "+Thread.currentThread().getId());
			}

			// each thread needs its own PostMethod
			postMethod = new HttpPost(httpClient.getJdUrl());
			// Set request timeout
			//postMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, duccComponent.getTimeout());
	   	} catch( Throwable t) {
	   		error = true;
	   		synchronized(JobProcessComponent.class) {
	   			// send notification to an agent 
	   			duccComponent.setState(ProcessState.FailedInitialization);
			}
	   		logger.error("HttpWorkerThread.run()", null, t);
	   		logger.info("HttpWorkerThread.run()", null,"EXITING WorkThread ID:"
					+ Thread.currentThread().getId());
	   		logger.warn("HttpWorkerThread.run()", null, "The Job Process Terminating Due To Initialization Error");
			/* *****************************************/
			/* *****************************************/
			/* *****************************************/
        	/*       EXITING  PROCESS ON FIRST ERROR   */
			/* *****************************************/
	   		try {
	   			// allow agent some time to process FailedInitialization event 
	   			Thread.sleep(2000);
	   		} catch( Exception e) {}
	//   		System.exit(1);
			/* *****************************************/
			/* *****************************************/
			/* *****************************************/
			/* *****************************************/
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
			IMetaTaskTransaction transaction=null;
			int major = 0;
			int minor = 0;
			// Enter process loop. Stop this thread on the first process error.
			while (duccComponent.isRunning()) {  

				try {
					major = IdGenerator.addAndGet(1);
					minor = 0;
					// the getWork() may block if connection is lost. 
					transaction = getWork(postMethod, major, minor);
					// first check if we are still running
					if ( !duccComponent.isRunning() ) {
    					logger.info("run", null,"Thread:"+Thread.currentThread().getId()+" Process is Stopping - Terminating This Thread");
    					break;
					}
					if ( !logConnectionToJD ) {
						logConnectionToJD = true;   // reset flag in case we loose connection to JD in the future
						logger.info("run", null, "T["+Thread.currentThread().getId()+"] - Regained Connection to JD");
					}
                    
					// If the client did not provide a Work Item, reduce frequency of Get requests
					// by sleeping in between Get's. Synchronize so only one thread is polling for work

                    // if the JD did not provide a Work Item, most likely the CR is
					// done. In such case, reduce frequency of Get requests
					// by sleeping in between Get's. Eventually the OR will 
					// deallocate this process and the thread will exit
					if ( transaction.getMetaTask() == null || transaction.getMetaTask().getUserSpaceTask() == null) {
					  logger.info("run", null, "Client is out of work - will retry quietly every",duccComponent.getThreadSleepTime()/1000,"secs.");
    					
    			  // Retry at the start of this block as another thread may have just exited with work
    				// so the TAS (or JD) may now have a lot of work.	
 						synchronized (HttpWorkerThread.class) {
							while(duccComponent.isRunning() ) {
							  transaction = getWork(postMethod, major, ++minor);
							  if ( transaction.getMetaTask() != null && transaction.getMetaTask().getUserSpaceTask() != null ) {
							    logger.info("run", null,"Thread:"+Thread.currentThread().getId()+" work flow has restarted");
							    break;
							  }
								waitAwhile(duccComponent.getThreadSleepTime());
							}
						}
						
					} 
					if ( duccComponent.isRunning()) {
						boolean workItemFailed = false;
						// process the Work item. Any exception here will cause the 
						// thread to terminate and also the JP to stop. The stopping
						// is orderly allowing each thread to finish processing of
						// the current WI. Once the JP notifies the Agent of a problem
						// the Agent will wait for 1 minute (default) before killing
						// this process via kill -9
						try {
							// To support investment reset we need to store transaction
							// object under a known key. This key is stored in the CAS.
							// In order to get to it, we need to deserialize the CAS 
							// in the user container. When an asynchronous investment
							// reset call is made from the user code, it will contain
							// that key to allow us to look up original transaction so that
							// we can send reset request to the JD.
							String key = (String)
									getKeyMethod.invoke(processorInstance, transaction.getMetaTask().getUserSpaceTask());
							if ( key != null ) {
								// add transaction under th
								transactionMap.put(key, transaction);
							}
							// make sure the JP is in the Running state before calling process()
							if ( !duccComponent.isRunning() ) {
								break;
							}
							//    ********** PROCESS() **************
							// using java reflection, call process to analyze the CAS. While 
							// we are blocking, user code may issue investment reset asynchronously.
							 List<Properties> metrics = (List<Properties>)processMethod.
							   invoke(processorInstance, transaction.getMetaTask().getUserSpaceTask());
							//    ***********************************
							if ( key != null ) {
                                // process ended we no longer expect investment reset from user
								// so remove transaction from the map
								transactionMap.remove(key);
							}
							
		                    logger.debug("run", null,"Thread:"+Thread.currentThread().getId()+" process() completed");
							//PerformanceMetrics metricsWrapper =
							//		new PerformanceMetrics();
						//	metricsWrapper.set(metrics);
							IServiceResultSerializer deserializer =
									new UimaResultDefaultSerializer();
							
							/*
							 				p.setProperty("name", metrics.getName());
				p.setProperty("uniqueName", metrics.getUniqueName());
				p.setProperty("analysisTime",
						String.valueOf(metrics.getAnalysisTime()));
				p.setProperty("numProcessed",
						String.valueOf(metrics.getNumProcessed()));

							 */
							List<PerformanceMetrics> pmList = new ArrayList<PerformanceMetrics>();
							for( Properties p : metrics) {
								PerformanceMetrics pm = 
										new PerformanceMetrics(p.getProperty("name"), p.getProperty("uniqueName"), Long.parseLong(p.getProperty("analysisTime")),0);
								pmList.add(pm);
							}
							
							transaction.getMetaTask().setPerformanceMetrics(deserializer.serialize(pmList));
							
						}  catch( InvocationTargetException ee) {
							
							logger.error("run", null, ee);
							// This is process error. It may contain user defined
							// exception in the stack trace. To protect against
						    // ClassNotFound, the entire stack trace was serialized.
							// Fetch the serialized stack trace and pass it on to
							// to the JD. The actual serialized stack trace is wrapped in
							// RuntimeException->AnalysisEngineException.message
							workItemFailed = true;
							// if WI processing fails while the service changes states to !Running
							// ignore results and terminate this thread.
							if ( !duccComponent.isRunning() ) {
								break;
							}
							IMetaTask mc = transaction.getMetaTask();
							//byte[] serializedException = null;
							Method getLastSerializedErrorMethod = processorInstance.getClass().getDeclaredMethod("getLastSerializedError");
							byte[] serializedException =
							    (byte[])getLastSerializedErrorMethod.invoke(processorInstance);
			
							mc.setUserSpaceException(serializedException);								

							logger.info("run", null, "Work item processing failed - returning serialized exception to the JD");
						} catch( Exception ee) {
							workItemFailed = true;
							// if WI processing fails while the service changes states to !Running
							// ignore results and terminate this thread.
							if ( !duccComponent.isRunning() ) {
								logger.info("run", null, "Work item processing failed - terminating thread - ignore any AE errors that may happen beyond this point");
								break;
							}
							// Serialize exception for the JD.
							byte[] serializedException = serializeException(ee);
							/*
							ByteArrayOutputStream baos = new ByteArrayOutputStream();
						    ObjectOutputStream oos = new ObjectOutputStream( baos );
						    oos.writeObject( ee);
						    oos.close();
							transaction.getMetaCas().setUserSpaceException(baos.toByteArray());
						    */
							logger.error("run", null, ee);
							transaction.getMetaTask().setUserSpaceException(serializedException);								
						}
						// Dont return serialized CAS to reduce the msg size
						transaction.getMetaTask().setUserSpaceTask(null);
						transaction.setType(Type.End);
						//String command = Type.End.name();
						
						minor++; // getWork()  
						TransactionId tid = new TransactionId(major, minor++);
						transaction.setTransactionId(tid);

						// if WI processing fails while the service changes states to !Running
						// ignore results and terminate this thread.
						if ( !duccComponent.isRunning() ) {
							break;
						}

						httpClient.execute(transaction, postMethod); // Work Item Processed - End
						// the execute() can block while recovering lost connection.
						// first check if we are still running.
						if ( !duccComponent.isRunning() ) {
	    					logger.info("run", null,"Thread:"+Thread.currentThread().getId()+" Process is Stopping - Terminating This Thread");
	    					break;
						}

						String wid = null;
                    	try {
                    		wid = transaction.getMetaTask().getSystemKey();
                    	} catch( Exception e) {
                    		
                    	}
	                    logger.info("run", null,"Thread:"+Thread.currentThread().getId()+" sent END for WI:"+wid);
	                    if ( exitOnProcessFailure && workItemFailed ) {
	                        if ( wid != null ) {
		                    	logger.warn("run", null, "Worker thread exiting due to error while processing WI:"+wid);
	                        } else {
		                    	logger.warn("run", null, "Worker thread exiting due to error while processing a WI");
	                        }
	        				logger.info("run", null, "JP Terminating Due to WI Error - Notify Agent");

        					// send an update to the agent.
	                        duccComponent.setState(ProcessState.Stopping, ReasonForStoppingProcess.ExceededErrorThreshold.toString());
        					// sleep for awhile to let the agent handle 
	                        // Stopping event. 
	                        // Reason for the sleep: there may be a race condition
	                        // here, where the JP sends a Stopping event to 
	                        // its agent and immediately exits. Before the
	                        // agent finishes handling of Stopping event its
	                        // internal thread detects process termination 
	                        // and may mark the JP as croaked. Sleep should
	                        // reduce the risk of this race but there is still 
	                        // a chance that agent doesn't handle Stopping
	                        // event before it detects JP terminating. Unlikely
	                        // but theoretically possible.
	                        try {
    	                        Thread.sleep(3000);
        					} catch( InterruptedException e) {}
        					/* *****************************************/
        					/* *****************************************/
        					/* *****************************************/
                        	/*       EXITING  PROCESS ON FIRST ERROR   */
        					/* *****************************************/
        					logger.warn("run", null,"Terminating Job Process - Work Item Failed");

        					// Stop the JVM hard. 
        					Runtime.getRuntime().halt(-1);
        					/* *****************************************/
        					/* *****************************************/
        					/* *****************************************/
        					/* *****************************************/

                        	break;
                        }
	                    maxFrameworkFailures.set(maxFrameworkErrors);   // reset framework failures on success
					} 
				} catch( InterruptedException e) {
					logger.error("run", null, "WorkerThread Interrupted - Terminating Thread "+Thread.currentThread().getId());
					return;
				} catch (Exception e ) {
					logger.error("run", null, e);
					// If max framework error count has been reached 
					// just exit the process
					if ( maxFrameworkFailures.decrementAndGet() <= 0 ) {
						logger.error("run", null, "The Job Process Terminating Due To a Framework Error");
						Runtime.getRuntime().halt(-1);
					}
				} finally {

				}

			}

		} catch (Throwable t) {
			logger.error("run", null, t);
		} finally {
			logger.warn("run",null,"EXITING WorkThread ID:"
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
					Method stopMethod = processorInstance.getClass().getSuperclass().getDeclaredMethod("stop");
					stopMethod.invoke(processorInstance);
				}
		   	} catch( Throwable t) {
		   		logger.warn("run",null,t);
		   	} finally {
				workerThreadCount.countDown();
		   	}
		
		}

	}
	private byte[] serializeException(Throwable t) {
		byte[] serializedException;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    ObjectOutputStream oos = null;
	    try {
	       oos = new ObjectOutputStream(baos);
	       oos.writeObject(t);
	       serializedException = baos.toByteArray();
        } catch (Exception ee) {
        	Exception e2 = new RuntimeException("Ducc Service Failed to Serialize the Cause of Process Failure. Check Service Log for Details");
        	try {
        		oos.writeObject(e2);
    		} catch( Exception ex ) {}
        	
        	serializedException = baos.toByteArray();
        } finally {
        	if ( oos != null ) {
        		try {
        			oos.close();
        		} catch( Exception ex ) {}
        	
        	}
        }
	    return serializedException;
	}

}
