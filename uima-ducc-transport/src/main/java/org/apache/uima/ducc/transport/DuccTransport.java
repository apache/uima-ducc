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
package org.apache.uima.ducc.transport;

import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.springframework.context.annotation.Scope;


//@Configuration
@Scope("prototype")
public class DuccTransport {
//	private String requestEndpoint;
	private DuccEventDispatcher dispatcher;
	
	/**
	 * Injects EventDispatcher for sending events to external components
	 *  
	 * @param dispatcher
	 */
	public void setDuccEventDispatcher(DuccEventDispatcher dispatcher ) {
		this.dispatcher = dispatcher;
	}
	public DuccEventDispatcher getDuccEventDispatcher() {
		return dispatcher;
	}
//	public void setRequestEndpoint(String requestEndpoint) {
//		this.requestEndpoint = requestEndpoint;
//	}
	
//    public RouteBuilder routeBuilderForIncomingRequests(@Qualifier("agentDelegateListener")final DuccEventDelegateListener delegate) {
//    public RouteBuilder routeBuilderForIncomingRequests(final DuccEventDelegateListener delegate) {
//        return new RouteBuilder() {
//            public void configure() {
//            	System.out.println("----------------> Request Endpoint:"+requestEndpoint+" Delegate Listener Type:"+delegate.getClass().getName());
//            	from(requestEndpoint)
//            	//.filter(header("target-nodes").//tokenize(",").isEqualTo(""))
//            	//.filter().method("myBean", "isGoldCustomer")
//            	.unmarshal().xstream()
//            	.process(new TransportProcessor())
//            	.bean(delegate);
//            }
//        };
//    }
//	
//	public class TransportProcessor implements Processor {
//
//		public void process(Exchange exchange) throws Exception {
//			System.out.println("... transport received Event. Body Type:"+exchange.getIn().getBody().getClass().getName());
//			Destination replyTo = exchange.getIn().getHeader("JMSReplyTo", Destination.class); 
//			System.out.println("... transport - value of replyTo:" + replyTo);
//		}
//		
//	}
}
