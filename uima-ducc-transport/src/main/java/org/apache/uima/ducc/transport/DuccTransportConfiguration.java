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

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;


@Configuration
@Scope("prototype")
public class DuccTransportConfiguration {
	@Value("#{ systemProperties['ducc.broker.url'] }")String brokerUrl;
	private static ActiveMQComponent duccAMQComponent = null;
	
	public void configureJMSTransport(String endpoint, CamelContext context) throws Exception {
	  
	  synchronized(ActiveMQComponent.class) {
	    if ( duccAMQComponent == null ) {
	      duccAMQComponent = ActiveMQComponent.activeMQComponent(brokerUrl);
	      context.addComponent("activemq",duccAMQComponent);
	    }
	  }
	}
	public DuccEventDispatcher duccEventDispatcher(String requestEndpoint,CamelContext context) throws Exception {
    configureJMSTransport(requestEndpoint, context);
		//  dont configure JMS for JP service wrapper which uses mina (sockets)
//	  if ( requestEndpoint != null && !requestEndpoint.startsWith("mina")) {
	//    configureJMSTransport(requestEndpoint, context);
		//}
		return new DuccEventDispatcher(context, requestEndpoint);
	}
}
