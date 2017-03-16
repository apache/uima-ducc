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
package org.apache.uima.ducc.sm.config;

import javax.jms.Destination;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.sm.ServiceManagerComponent;
import org.apache.uima.ducc.sm.event.ServiceManagerEventListener;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.event.AServiceRequest;
import org.apache.uima.ducc.transport.event.ServiceReplyEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


/**
 * A {@link ServiceManagerConfiguration} to configure Service Manager component. Depends on 
 * properties loaded by a main program into System properties. 
 * 
 */
@Configuration
@Import({DuccTransportConfiguration.class,CommonConfiguration.class})
public class ServiceManagerConfiguration 
{
	//	use Spring magic to autowire (instantiate and bind) CommonConfiguration to a local variable
	@Autowired CommonConfiguration common;
	//	use Spring magic to autowire (instantiate and bind) DuccTransportConfiguration to a local variable
	@Autowired DuccTransportConfiguration serviceManagerTransport;

    private DuccLogger logger = DuccLogger.getLogger(this.getClass(), "SM");
    private static DuccId jobid = null;

	/**
	 * Instantiate {@link ServiceManagerEventListener} which will handle incoming messages.
	 * 
	 * @param sm - {@link ServiceManagerComponent} instance
	 * @return - {@link ServiceManagerEventListener}
	 */
	public ServiceManagerEventListener serviceManagerDelegateListener(ServiceManagerComponent sm) {
		ServiceManagerEventListener smel =  new ServiceManagerEventListener(sm);
		//smel.setEndpoint(common.jmStateUpdateEndpoint);
		smel.setEndpoint(common.pmRequestEndpoint);
		return smel;
	}
	/**
	 * Create a Router to handle incoming messages from a given endpoint. All messages are delegated
	 * to a provided listener. Note: Camel uses introspection to determine which method to call when
	 * delegating a message. The name of the method doesnt matter it is the argument that needs
	 * to match the type of object in the message. If there is no method with a matching argument
	 * type the message will not be delegated.
	 * 
	 * @param endpoint - endpoint where messages are expected
	 * @param delegate - {@link ServiceManagerEventListener} instance
	 * @return - initialized {@link RouteBuilder} instance
	 * 
	 */
	public synchronized RouteBuilder routeBuilderForIncomingRequests(final String endpoint, final ServiceManagerEventListener delegate) {
        return new RouteBuilder() {
            public void configure() {
            	from(endpoint)
            	//from("activemq:topic:tmp-jm-state")
            	.process(new TransportProcessor())
            	.bean(delegate);
            }
        };
    }

	/**
	 * @param endpoint - endpoint where messages are expected
	 * @param delegate - {@link ServiceManagerEventListener} instance
	 * @return - initialized {@link RouteBuilder} instance
	 */
	  
	public synchronized RouteBuilder routeBuilderForApi(final String endpoint, final ServiceManagerEventListener delegate) 
	{
        return new RouteBuilder() {
            public void configure() {
            	from(endpoint)
                    //from("activemq:topic:tmp-jm-state")
                    .process(new TransportProcessor())
                    .bean(delegate)
                    .process(new SmReplyProcessor())   // inject reply object
                    ;
            }
        };
    }

	private class SmReplyProcessor implements Processor {
		
		private SmReplyProcessor() {
		}
		
		public void process(Exchange exchange) throws Exception 
		{
            String methodName = "process";                
            try {
                logger.info(methodName, null, "Replying");
                AServiceRequest incoming =  (AServiceRequest) exchange.getIn().getBody();
                ServiceReplyEvent reply = incoming.getReply();
                exchange.getIn().setBody(reply);
            } catch ( Throwable t ) {
                logger.error(methodName, null, t);
            }
		}
	}

    public class ErrorProcessor implements Processor {
        
        public void process(Exchange exchange) throws Exception {
            // the caused by exception is stored in a property on the exchange
            Throwable caused = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
            try {
                logger.error("ErrorProcessor.process", null, caused);
                exchange.getOut().setBody(XStreamUtils.marshall(caused));
            	
            } catch( Throwable t ) {
                logger.error("ErrorProcessor.process", null,t);
            }
        }
    }

	/**
	 * This class handles a message before it is delegated to a component listener. In this method the message can be enriched,
	 * logged, etc.
	 * 
	 */
	public class TransportProcessor implements Processor {

		public void process(Exchange exchange) throws Exception {
			String location = "process";
			String text = "... SM transport received Event. Body Type:"+exchange.getIn().getBody().getClass().getName();
			logger.debug(location, jobid, text);
			Destination replyTo = exchange.getIn().getHeader("JMSReplyTo", Destination.class); 
			text = "... transport - value of replyTo:" + replyTo;
			logger.debug(location, jobid, text);
		}
		
	}
	
	
	/**
	 * Creates Camel router that will publish Service Manager state at regular intervals.
	 * 
	 * @param targetEndpointToReceiveSMStateUpdate - endpoint where to publish SM state 
	 * @param statePublishRate - how often to publish state
	 * @return
	 * @throws Exception
     * @deprecated
	 *
	private RouteBuilder routeBuilderForSMStatePost(final ServiceManager sm, final String targetEndpointToReceiveSMStateUpdate, final int statePublishRate) throws Exception {
		final ServiceManagerStateProcessor smp =  // an object responsible for generating the state 
			new ServiceManagerStateProcessor(sm);
		
		return new RouteBuilder() {
		      public void configure() {
		        from("timer:smStateDumpTimer?fixedRate=true&period=" + statePublishRate)
		                .process(smp)
		                .marshal()
		                .xstream()
		                .to(targetEndpointToReceiveSMStateUpdate);
		      }
		    };

	}
	*/
	/**
	 * Camel Processor responsible for generating Service Manager's state.
	 * 
	 *
	private class ServiceManagerStateProcessor implements Processor {
		private ServiceManager sm;
		
		private ServiceManagerStateProcessor(ServiceManager sm) {
			this.sm = sm;
		}
		public void process(Exchange exchange) throws Exception {
			// Fetch new state from Job Manager
			SmStateDuccEvent sse = sm.getState();
			//	Add the state object to the Message
			exchange.getIn().setBody(sse);
		}
		
	}
	*/
	
	/**
	 * Creates and initializes {@link ServiceManagerComponent} instance. @Bean annotation identifies {@link ServiceManagerComponent}
	 * as a Spring framework Bean which will be managed by Spring container.  
	 * 
	 * @return {@link ServiceManagerComponent} instance
	 * 
	 * @throws Exception
	 */
	@Bean 
	public ServiceManagerComponent serviceManager() throws Exception {
		ServiceManagerComponent sm = new ServiceManagerComponent(common.camelContext());
        //	Instantiate delegate listener to receive incoming messages. 
        ServiceManagerEventListener delegateListener = this.serviceManagerDelegateListener(sm);
		//	Inject a dispatcher into the listener in case it needs to send
		//  a message to another component
        // delegateListener.setDuccEventDispatcher(serviceManagerTransport.duccEventDispatcher(common.orchestratorStateUpdateEndpoint, sm.getContext()));

        // Set context so SM can send state when it wants to (not on timer)
        sm.setTransportConfiguration(serviceManagerTransport.duccEventDispatcher(common.smStateUpdateEndpoint, sm.getContext()), 
                                     common.smStateUpdateEndpoint);
        // OR state messages - incoming
		sm.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.orchestratorStateUpdateEndpoint, delegateListener));
		// API requests - incoming (via OR)
		sm.getContext().addRoutes(this.routeBuilderForApi(common.smApiEndpoint, delegateListener));

        // TODO Not used - timer to send state. We now send whenever we get an OR heartbeat.
		//sm.getContext().addRoutes(this.routeBuilderForSMStatePost(sm, common.smStateUpdateEndpoint, Integer.parseInt(common.smStatePublishRate)));
		return sm;
	}

}
