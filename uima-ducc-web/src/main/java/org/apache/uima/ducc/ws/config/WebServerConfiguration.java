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
package org.apache.uima.ducc.ws.config;

import javax.jms.ObjectMessage;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.ByteSequence;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsMessage;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.ws.DuccBoot;
import org.apache.uima.ducc.ws.WebServerComponent;
import org.apache.uima.ducc.ws.event.WebServerEventListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


/**
 * A {@link WebServerConfiguration} to configure JobDriver component. Depends on 
 * properties loaded by a main program into System properties. 
 * 
 */
@Configuration
@Import({DuccTransportConfiguration.class,CommonConfiguration.class})
public class WebServerConfiguration {
	//	use Spring magic to autowire (instantiate and bind) CommonConfiguration to a local variable
	@Autowired CommonConfiguration common;
	//	use Spring magic to autowire (instantiate and bind) DuccTransportConfiguration to a local variable
	@Autowired DuccTransportConfiguration webServerTransport;
	
	private DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(WebServerConfiguration.class.getName());
	private DuccId jobid = null;
	
	/**
	 * Instantiate {@link WebServerEventListener} which will handle incoming messages.
	 * 
	 * @param ws - {@link WebServerComponent} instance
	 * @return - {@link WebServerEventListener}
	 */
	public WebServerEventListener webServerDelegateListener(WebServerComponent ws) {
		WebServerEventListener wsel =  new WebServerEventListener(ws);
		return wsel;
	}
	/**
	 * Create a Router to handle incoming messages from a given endpoint. All messages are delegated
	 * to a provided listener. Note: Camel uses introspection to determine which method to call when
	 * delegating a message. The name of the method doesnt matter it is the argument that needs
	 * to match the type of object in the message. If there is no method with a matching argument
	 * type the message will not be delegated.
	 * 
	 * @param endpoint - endpoint where messages are expected
	 * @param delegate - {@link WebServerEventListener} instance
	 * @return - initialized {@link RouteBuilder} instance
	 * 
	 */
	public synchronized RouteBuilder routeBuilderForIncomingRequests(final String endpoint, final WebServerEventListener delegate) {
        return new RouteBuilder() {
        	Processor p = new AccessJmsBody();
            public void configure() {
            	from(endpoint)
            	.process(p)
            	//from("activemq:topic:tmp-jm-state")
            	.bean(delegate);
            }
        };
    }
	
	/**
	 * Creates and initializes {@link WebServerComponent} instance. @Bean annotation identifies {@link WebServerComponent}
	 * as a Spring framework Bean which will be managed by Spring container.  
	 * 
	 * @return {@link WebServerComponent} instance
	 * 
	 * @throws Exception
	 */
	@Bean 
	public WebServerComponent webServer() throws Exception {
		String methodName = "webServer";
		WebServerComponent ws = null;
		try {
			ws = new WebServerComponent(common.camelContext(), common);
			DuccBoot.boot(common);
			//	Instantiate delegate listener to receive incoming messages. 
			WebServerEventListener delegateListener = this.webServerDelegateListener(ws);
			//	Inject a dispatcher into the listener in case it needs to send
			//  a message to another component
			delegateListener.setDuccEventDispatcher(webServerTransport.duccEventDispatcher(common.orchestratorStateUpdateEndpoint, ws.getContext()));
			//	Inject Camel Router that will delegate messages to WebServer delegate listener
			ws.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.orchestratorStateUpdateEndpoint, delegateListener));
			ws.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.nodeMetricsEndpoint, delegateListener));
			ws.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.rmStateUpdateEndpoint, delegateListener));
			ws.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.smStateUpdateEndpoint, delegateListener));
			ws.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.pmStateUpdateEndpoint, delegateListener));
			ws.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.dbComponentStateUpdateEndpoint, delegateListener));
		}
		catch(Throwable t) {
			duccLogger.error(methodName, jobid, t);
		}
		return ws;
	}
    public class AccessJmsBody implements Processor
    {
        int msglen = 0;
        String objectName = "unknown";
        public void process(Exchange exchange) throws Exception {
        	String location = "AccessJmsBody:process";
            try {
                Object o = exchange.getIn();
                if ( o instanceof JmsMessage ) {
                    JmsMessage msg =  (JmsMessage) o;
                    o = msg.getJmsMessage();
                    if ( o instanceof ActiveMQMessage ) {
                        ActiveMQMessage amqMessage = (ActiveMQMessage) o;
                        if ( amqMessage instanceof ObjectMessage ) {
                            Object body = ((ObjectMessage)amqMessage).getObject();
                            ByteSequence bs = amqMessage.getContent();
                            msglen = bs.getLength();
                            objectName = body.getClass().getName();
                        }
                    }
                }
                Long pubSize = new Long(msglen);
                exchange.getIn().setHeader("pubSize", pubSize);
                String text = "Message length is " + msglen + " for " + objectName;
                duccLogger.debug(location, jobid, text);
            } 
            catch (Throwable t) {
            	duccLogger.error(location, jobid, t);
            }
        }
    }

}
