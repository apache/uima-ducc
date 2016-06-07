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
package org.apache.uima.ducc.pm.config;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultClassResolver;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.config.DuccBlastGuardPredicate;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.pm.ProcessManager;
import org.apache.uima.ducc.pm.ProcessManagerComponent;
import org.apache.uima.ducc.pm.event.ProcessManagerEventListener;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.PmStateDuccEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.thoughtworks.xstream.XStream;

/**
 * A {@link ProcessManagerConfiguration} to configure Process Manager component. Depends on 
 * properties loaded by a main program into System properties. 
 * 
 */
@Configuration
@Import({DuccTransportConfiguration.class,CommonConfiguration.class})
public class ProcessManagerConfiguration {
	private static DuccLogger logger = new DuccLogger(ProcessManagerConfiguration.class, "ProcessManagerConfiguration");
	//	use Spring magic to autowire (instantiate and bind) CommonConfiguration to a local variable
	@Autowired CommonConfiguration common;
	//	use Spring magic to autowire (instantiate and bind) DuccTransportConfiguration to a local variable
	@Autowired DuccTransportConfiguration processManagerTransport;
	
	/**
	 * Instantiate {@link ProcessManagerEventListener} which will handle incoming messages.
	 * 
	 * @param pm - {@link ProcessManagerComponent} instance
	 * @return - {@link ProcessManagerEventListener}
	 */
	public ProcessManagerEventListener processManagerDelegateListener(ProcessManagerComponent pm) {
		ProcessManagerEventListener pmel =  new ProcessManagerEventListener(pm);
		pmel.setEndpoint(common.agentRequestEndpoint);
		return pmel;
	}
	/**
	 * Create a Router to handle incoming messages from a given endpoint. All messages are delegated
	 * to a provided listener. Note: Camel uses introspection to determine which method to call when
	 * delegating a message. The name of the method doesnt matter it is the argument that needs
	 * to match the type of object in the message. If there is no method with a matching argument
	 * type the message will not be delegated.
	 * 
	 * @param endpoint - endpoint where messages are expected
	 * @param delegate - {@link ProcessManagerEventListener} instance
	 * @return - initialized {@link RouteBuilder} instance
	 * 
	 */
	public synchronized RouteBuilder routeBuilderForIncomingRequests(final String endpoint, final ProcessManagerEventListener delegate, final ProcessManagerComponent pm) {
        return new RouteBuilder() {
          
            public void configure() {
            	System.out.println("Process Manager waiting for messages on endpoint:"+endpoint);
              onException(Throwable.class).
                maximumRedeliveries(0).  // dont redeliver the message
                  handled(false).  // the caller will receive the exception
                    process(new ErrorProcessor());   // delegate exception to the handler
            	
              from(endpoint)
            	.bean(delegate);
            }
        };
    }
	
  public class ErrorProcessor implements Processor {

    public void process(Exchange exchange) throws Exception {
      // the caused by exception is stored in a property on the exchange
      Throwable throwable = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
      logger.error("ErrorProcessor.process",null, throwable);
      throwable.printStackTrace();
    }
  }
	/**
	 * This class handles a message before it is delegated to a component listener. In this method the message can be enriched,
	 * logged, etc.
	 * 
	 */
	public class TransportProcessor implements Processor {

		public void process(Exchange exchange) throws Exception {
			String methodName="process";
			
    		logger.info(methodName, null,"Transport received Event. Body Type:"+exchange.getIn().getBody().getClass().getName());
//			Destination replyTo = exchange.getIn().getHeader("JMSReplyTo", Destination.class); 
//			System.out.println("... transport - value of replyTo:" + replyTo);
		}
		
	}
	public class DebugProcessor implements Processor {
		private ProcessManagerComponent pm;
		public DebugProcessor(ProcessManagerComponent pm) {
			this.pm = pm;
		}
		public void process(Exchange exchange) throws Exception {
			String methodName="process";
			if ( pm.getLogLevel().toLowerCase().equals("trace")) {
				XStreamDataFormat xStreamDataFormat = new XStreamDataFormat();
				xStreamDataFormat.setPermissions("*");
		        XStream xStream = xStreamDataFormat.getXStream(new DefaultClassResolver());
				String marshalledEvent = xStream.toXML(exchange.getIn().getBody());
				pm.logAtTraceLevel(methodName, marshalledEvent);
			}
//			if ( logger.isDebug() ) {
//				XStreamDataFormat xStreamDataFormat = new XStreamDataFormat();
//		        XStream xStream = xStreamDataFormat.getXStream(new DefaultClassResolver());
//				String marshalledEvent = xStream.toXML(exchange.getIn().getBody());
//				logger.debug(methodName, null,marshalledEvent);
//			}
//			Destination replyTo = exchange.getIn().getHeader("JMSReplyTo", Destination.class); 
//			System.out.println("... transport - value of replyTo:" + replyTo);
		}
		
	}
	
	/**
	 * Creates Camel router that will publish ProcessManager state at regular intervals.
	 * 
	 * @param targetEndpointToReceiveProcessManagerStateUpdate - endpoint where to publish PM state 
	 * @param statePublishRate - how often to publish state
	 * @return
	 * @throws Exception
	 */
	private RouteBuilder routeBuilderForProcessManagerStatePost(final ProcessManagerComponent pm, final String targetEndpointToReceiveProcessManagerStateUpdate, final int statePublishRate) throws Exception {
		final ProcessManagerStateProcessor pmsp =  // an object responsible for generating the state 
			new ProcessManagerStateProcessor(pm);
		
		return new RouteBuilder() {
		      public void configure() {
		    	String methodName = "configure";
          final Predicate blastGuard = new DuccBlastGuardPredicate(pm.getLogger());

		    	logger.trace(methodName, null,"timer:pmStateDumpTimer?fixedRate=true&period=" + statePublishRate);
		    	logger.trace(methodName, null,"endpoint=" + targetEndpointToReceiveProcessManagerStateUpdate);
		        from("timer:pmStateDumpTimer?fixedRate=true&period=" + statePublishRate)
              // This route uses a filter to prevent sudden bursts of messages which
              // may flood DUCC daemons causing chaos. The filter disposes any message
              // that appears in a window of 1 sec or less.
              .filter(blastGuard)
                    .process(pmsp)
                    .to(targetEndpointToReceiveProcessManagerStateUpdate);

		      }
		    };
	}
	
	/**
	 * Camel Processor responsible for generating ProcessManager's state.
	 */
	private class ProcessManagerStateProcessor implements Processor {
		private ProcessManager pm;
		
		private ProcessManagerStateProcessor(ProcessManager pm) {
			this.pm = pm;
		}
		public void process(Exchange exchange) throws Exception {
			// Fetch new state from ProcessManager
			PmStateDuccEvent jse = pm.getState();
			//	Add the state object to the Message
			exchange.getIn().setBody(jse);
		}
	}
	
	/**
	 * Creates and initializes {@link ProcessManagerComponent} instance. @Bean annotation identifies {@link ProcessManagerComponent}
	 * as a Spring framework Bean which will be managed by Spring container.  
	 * 
	 * @return {@link ProcessManagerComponent} instance
	 * 
	 * @throws Exception
	 */
	@Bean 
	public ProcessManagerComponent processManager() throws Exception {
        CamelContext camelContext = common.camelContext();
		DuccEventDispatcher eventDispatcher = processManagerTransport.duccEventDispatcher(common.agentRequestEndpoint, camelContext);
		logger.info("processManager()",null, "PM publishes state update to Agents on endpoint:"+common.agentRequestEndpoint);
		ProcessManagerComponent pm = new ProcessManagerComponent(camelContext, eventDispatcher);
        //	Instantiate delegate listener to receive incoming messages. 
        ProcessManagerEventListener delegateListener = this.processManagerDelegateListener(pm);
        //	Inject a dispatcher into the listener in case it needs to send
		//  a message to another component
		delegateListener.setDuccEventDispatcher(eventDispatcher);
		//	Inject Camel Router that will delegate messages to Process Manager delegate listener
		pm.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.orchestratorStateUpdateEndpoint, delegateListener, pm));
		pm.getContext().addRoutes(this.routeBuilderForProcessManagerStatePost(pm, common.pmStateUpdateEndpoint, Integer.parseInt(common.pmStatePublishRate)));
		return pm;
	}

}
