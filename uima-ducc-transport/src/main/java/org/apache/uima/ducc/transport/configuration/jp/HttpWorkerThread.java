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

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.container.jp.JobProcessManager;
import org.apache.uima.ducc.container.jp.iface.IUimaProcessor;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IPerformanceMetrics;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.container.net.impl.PerformanceMetrics;

public class HttpWorkerThread implements Runnable {
	DuccHttpClient httpClient = null;
	private IUimaProcessor uimaProcessor;
	private JobProcessComponent duccComponent;
	static AtomicInteger counter = new AtomicInteger();
    private DuccLogger logger;
	private Object monitor = new Object();
	private CountDownLatch workerThreadCount = null;
	private JobProcessManager jobProcessManager = null;
/*
	interface SMEvent {
		Event action();
		State nextState();
	}
	
	interface Event {
		State action(SMContext ctx);
	}
	
	enum Events implements Event {
		
		GetWI {

			public State action(SMContext ctx) {
				try {
					ctx.setEvent(Events.GetReply);
					return States.GetPending;
				} catch( Exception e) {
					return Events.SendFailed.action(ctx);
				}
				
			}
		},
		GetReply {
			public State action(SMContext ctx) {
				
				ctx.setEvent(Events.AckReply);
				return States.CasReceived;
			}
		},
		GetRequest {

			@Override
			public Event action(SMContext ctx) {
				// TODO Auto-generated method stub
				return this;
			}
			
		},
		AckReply {

			@Override
			public State action(SMContext ctx) {
				
				return States.CasReceived;
			}
			
		},
		AckRequest {

			@Override
			public Event action(SMContext ctx) {
				// TODO Auto-generated method stub
				return this;
			}
			
		},
		EndReply {

			@Override
			public Event action(SMContext ctx) {
				// TODO Auto-generated method stub
				return this;
			}
			
		},
		EndRequest {

			@Override
			public Event action(SMContext ctx) {
				// TODO Auto-generated method stub
				return this;
			}
			
		},
		PipelineEnded {

			@Override
			public Event action(SMContext ctx) {
				// TODO Auto-generated method stub
				return this;
			}
			
		},
		ReportRequest {

			@Override
			public Event action(SMContext ctx) {
				// TODO Auto-generated method stub
				return this;
			}
			
		},
		Timeout {

			@Override
			public Event action(SMContext ctx) {
				// TODO Auto-generated method stub
				return this;
			}
			
		},
		SendFailed {

			@Override
			public State action(SMContext ctx) {
				// TODO Auto-generated method stub
				return this;
			}
			
		},
	}
	class SMContextImpl implements SMContext {
		State state;
		Event event;
		DuccHttpClient httpClient;
		SMContextImpl(DuccHttpClient httpClient, State initialState) {
			state = initialState;
			this.httpClient = httpClient;
		}
		@Override
		public State state() {
			return state;
		}

		@Override
		public void nextState(State state) {
			this.state = state;
		}
		public void setEvent(Event event) {
			this.event = event;
		}
		public DuccHttpClient getClient() {
			return httpClient;
		}
	};
	
	interface SMContext {
		State state();
		Event event();
		public DuccHttpClient getClient();
		void setEvent(Event event);
		void nextState(State state);
	}
	interface State {
		boolean process(SMContext ctx);
	}
	
	public enum States implements State {
		Start {
			public boolean process(SMContext ctx) {
				ctx.nextState(ctx.event().action(ctx));
				return true;
			}
			
		},
		GetPending {
			public boolean process(SMContext ctx) {
				ctx.nextState(States.CasReceived);
				return true;
			}
			
		},
		CasReceived {
			public boolean process(SMContext ctx) {
				ctx.nextState(States.CasActive);
				return true;
			}

		},
		CasActive {
			public boolean process(SMContext ctx) {
				ctx.nextState(States.CasEnd);
				return true;
			}
			
		},
		CasEnd {
			public boolean process(SMContext ctx) {
				ctx.nextState(States.Start, Events.ProcessNext);
				return true;
			}

		}
		
	}
	*/
	public HttpWorkerThread(JobProcessComponent component, DuccHttpClient httpClient,
			JobProcessManager jobProcessManager , CountDownLatch workerThreadCount) {
		this.duccComponent = component;
		this.httpClient = httpClient;
		//this.uimaProcessor = processor;
		this.jobProcessManager = jobProcessManager;
		this.workerThreadCount = workerThreadCount;
	}
    private void initialize(boolean isUimaASJob ) throws Exception {
    	// For UIMA-AS job, there should only be one instance of UimaProcessor.
    	// This processor contains AMQ broker, UIMA-AS client and UIMA-AS service.
    	// For UIMA job, each AE must be pinned to a thread that called intialize().
    	synchronized(IUimaProcessor.class ) {
    		if ( isUimaASJob && uimaProcessor != null ) {
    			return; // for UIMA-AS job (DD) there is only one uimaProcessor
    		}
        	uimaProcessor = jobProcessManager.deploy();

    	}
    	
    }
	public void run() {
		try {
			initialize(duccComponent.isUimaASJob());
			//States stateMachine = new States(States.Start);
//			SMContext ctx = new SMContextImpl(httpClient, States.Start);
			String command="";
			// run forever (or until the process throws IllegalStateException
			while (duccComponent.isRunning()) {  //service.running && ctx.state().process(ctx)) {

				try {
					IMetaCasTransaction transaction = new MetaCasTransaction();
					
					// According to HTTP spec, GET may not contain Body in 
					// HTTP request. HttpClient actually enforces this. So
					// do a POST instead of a GET.
					transaction.setType(Type.Get);  // Tell JD you want a CAS
					command = Type.Get.name();
					transaction = httpClient.post(transaction);
                    
					// Confirm receipt of the CAS. 
					transaction.setType(Type.Ack);
					command = Type.Ack.name();
					httpClient.post(transaction); // Ready to process
					
					// if the JD did not provide a CAS, most likely the CR is
					// done. In such case, reduce frequency of Get requests
					// by sleeping in between Get's. Eventually the JD will 
					// confirm that there is no more work and this thread
					// can exit.
					if ( transaction.getMetaCas() == null || transaction.getMetaCas().getUserSpaceCas() == null) {
						// if the JD state is Ended, exit this thread as all work has
						// been processed and accounted for
						if ( transaction.getJdState().equals(JdState.Ended) ) {
							duccComponent.getLogger().warn("run", null, "Exiting Thread "+Thread.currentThread().getId()+" JD Finished Processing");
							System.out.println("Exiting Thred DriverState=Ended");
							break; // the JD completed. Exit the thread
						}
						// There is no CAS. It looks like the JD CR is done but there
						// are still WIs being processed. Slow down the rate of requests	
						synchronized (monitor) {
							try {
								monitor.wait(duccComponent.getThreadSleepTime());
							} catch (InterruptedException e) {

							}
						}
					} else {
						// process the CAS
						@SuppressWarnings("unchecked")
						List<Properties> metrics = (List<Properties>) 
								uimaProcessor.process(transaction.getMetaCas().getUserSpaceCas());
						
						IPerformanceMetrics metricsWrapper =
								new PerformanceMetrics();
						metricsWrapper.set(metrics);
						
						transaction.getMetaCas().setPerformanceMetrics(metricsWrapper);
						transaction.getMetaCas().setUserSpaceCas(null);
						transaction.setType(Type.End);
						command = Type.End.name();
						httpClient.post(transaction); // Work Item Processed - End
					}
				} catch( SocketTimeoutException e) {
					duccComponent.getLogger().warn("run", null, "Timed Out While Awaiting Response from JD for "+command+" Request - Retrying ...");
					System.out.println("Time Out While Waiting For a Reply from JD For "+command+" Request");
				}
				catch (Exception e ) {
					duccComponent.getLogger().warn("run", null, e);
					duccComponent.getLogger().warn("run", null, "Caught Unexpected Exception - Exiting Thread "+Thread.currentThread().getId() );
					e.printStackTrace();
					break; 
				} finally {

				}

			}

		} catch (Throwable t) {
			t.printStackTrace();
			duccComponent.getLogger().warn("run", null, t);
		} finally {
//			try {
//				if ( uimaProcessor != null ) {
//					uimaProcessor.stop();
//				}
//			} catch( Throwable t) {
//				
//			}
			System.out.println("EXITING WorkThread ID:"
					+ Thread.currentThread().getId());
			workerThreadCount.countDown();
		}

	}

}
