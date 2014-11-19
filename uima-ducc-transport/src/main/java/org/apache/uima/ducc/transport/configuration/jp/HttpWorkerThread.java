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

import org.apache.uima.ducc.container.jp.iface.IUimaProcessor;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Direction;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;

public class HttpWorkerThread implements Runnable {
	DuccHttpClient httpClient = null;
	private IUimaProcessor uimaProcessor;
	private JobProcessComponent duccComponent;
	
	private volatile boolean running = true;

	private Object monitor = new Object();
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
			IUimaProcessor processor) {
		this.duccComponent = component;
		this.httpClient = httpClient;
		this.uimaProcessor = processor;
	}

	public void run() {
		try {
			String xmi = null;
			//States stateMachine = new States(States.Start);
//			SMContext ctx = new SMContextImpl(httpClient, States.Start);
			
			// run forever (or until the process throws IllegalStateException
			while (true) {  //service.running && ctx.state().process(ctx)) {

				try {
					IMetaCasTransaction transaction = new MetaCasTransaction();
					
					// According to HTTP spec, GET may not contain Body in 
					// HTTP request. HttpClient actually enforces this. So
					// do a POST instead of a GET.
					transaction = httpClient.post(transaction);
					
					transaction.setDirection(Direction.Request);
					//httpClient.post(transaction); // Received Work Item
					// if the processor is stopped due to external request the
					// process() will throw IllegalStateException handled below.
					
				//	uimaProcessor.process(transaction.getMetaCas().getUserSpaceCas());
					httpClient.post(transaction); // Work Item Processed
					System.exit(0);
/*
					synchronized (monitor) {
						Random rand = new Random();

						// nextInt is normally exclusive of the top value,
						// so add 1 to make it inclusive
						int randomNum = rand.nextInt((1000 - 100) + 1) + 100;
						try {
							monitor.wait(randomNum);
						} catch (InterruptedException e) {

						}
					}
*/
				} catch (IllegalStateException e) {
					break; // service stopped
				} finally {

				}

			}

		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			System.out.println("EXITING WorkThread ID:"
					+ Thread.currentThread().getId());
		}

	}

}
