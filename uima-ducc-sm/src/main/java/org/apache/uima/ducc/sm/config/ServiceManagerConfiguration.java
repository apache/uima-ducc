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

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jetty.JettyHttpComponent;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.XStreamUtils;
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
	/*
	  
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
*/
    private RouteBuilder routeBuilderForJetty(final CamelContext context, final ServiceManagerEventListener delegate) throws Exception {
    
        return new RouteBuilder() {
            public void configure() {
            
                JettyHttpComponent jettyComponent = new JettyHttpComponent();
                String port = System.getProperty("ducc.sm.http.port");
                //ExchangeMonitor xmError = new ExchangeMonitor(LifeStatus.Error, ExchangeType.Receive);
			
                context.addComponent("jetty", jettyComponent);
                onException(Throwable.class).maximumRedeliveries(0).handled(false).process(new ErrorProcessor());
            
                from("jetty://http://0.0.0.0:" + port + "/sm")
                    .unmarshal().xstream()
                    .bean(delegate)
                    .process(new SmReplyProcessor())     // inject reply object
                    .process(new JettyReplyProcessor())  // translate to http response
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

    private class JettyReplyProcessor
        implements Processor 
    {
        public void process(Exchange exchange) throws Exception 
        {
            exchange.getOut().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
            exchange.getOut().setHeader("content-type", "text/xml");
            Object o = exchange.getIn().getBody();
            if ( o != null ) {
                String body = XStreamUtils.marshall(o);
                exchange.getOut().setBody(body);
                exchange.getOut().setHeader("content-length", body.length());
            } else {
                logger.warn("RouteBuilder.configure", null, new DuccRuntimeException("No reply object was provided."));
                exchange.getOut().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
            } 
        }
    }

    public class ErrorProcessor implements Processor {
        
        public void process(Exchange exchange) throws Exception {
            // the caused by exception is stored in a property on the exchange
            Throwable caused = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
            exchange.getOut().setBody(XStreamUtils.marshall(caused));
            logger.error("ErrorProcessor.process", null, caused);
        }
    }

	/**
	 * This class handles a message before it is delegated to a component listener. In this method the message can be enriched,
	 * logged, etc.
	 * 
	 */
	public class TransportProcessor implements Processor {

		public void process(Exchange exchange) throws Exception {
//			System.out.println("... SM transport received Event. Body Type:"+exchange.getIn().getBody().getClass().getName());
//			Destination replyTo = exchange.getIn().getHeader("JMSReplyTo", Destination.class); 
//			System.out.println("... transport - value of replyTo:" + replyTo);
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
        

        //delegateListener.setDuccEventDispatcher(serviceManagerTransport.duccEventDispatcher(common.orchestratorAbbreviatedStateUpdateEndpoint, sm.getContext()));
        //

		//	Inject Camel Router that will delegate messages to Service Manager delegate listener
        // TODO Not used? OR state messages - incoming
		// sm.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.orchestratorStateUpdateEndpoint, delegateListener));

        // OR abbreviated state messages - incoming
		sm.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.orchestratorAbbreviatedStateUpdateEndpoint, delegateListener));

        // API requests - incoming
		//sm.getContext().addRoutes(this.routeBuilderForApi(common.smRequestEndpoint, delegateListener));
		sm.getContext().addRoutes(this.routeBuilderForJetty(sm.getContext(), delegateListener));

        // TODO Not used - timer to send state. We now send whenever we get an OR heartbeat.
		//sm.getContext().addRoutes(this.routeBuilderForSMStatePost(sm, common.smStateUpdateEndpoint, Integer.parseInt(common.smStatePublishRate)));
		return sm;
	}

}
