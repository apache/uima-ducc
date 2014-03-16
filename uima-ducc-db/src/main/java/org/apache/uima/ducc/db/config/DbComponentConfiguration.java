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
package org.apache.uima.ducc.db.config;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.config.DuccBlastGuardPredicate;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.db.DbComponent;
import org.apache.uima.ducc.db.event.DbComponentEventListener;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.DbComponentStateEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * A {@link DbComponentConfiguration} to configure DB component. Depends on 
 * properties loaded by a main program into System properties. 
 * 
 */
@Configuration
@Import({DuccTransportConfiguration.class,CommonConfiguration.class})
public class DbComponentConfiguration {
	
	private static  DuccLogger logger = DuccLoggerComponents.getDbLogger(DbComponentConfiguration.class.getName());
	private DuccId jobid = null;
	
	//	use Spring magic to autowire (instantiate and bind) CommonConfiguration to a local variable
	@Autowired CommonConfiguration common;
	//	use Spring magic to autowire (instantiate and bind) DuccTransportConfiguration to a local variable
	@Autowired DuccTransportConfiguration transportConfiguration;
	
	/**
	 * Instantiate {@link DbComponentEventListener} which will handle incoming messages.
	 * 
	 * @param dbComponent - {@link DbComponent} instance
	 * @return - {@link DbComponentEventListener}
	 */
	public DbComponentEventListener processManagerDelegateListener(DbComponent dbComponent) {
		DbComponentEventListener dbListener =  new DbComponentEventListener(dbComponent);
		return dbListener;
	}
	/**
	 * Create a Router to handle incoming messages from a given endpoint. All messages are delegated
	 * to a provided listener. Note: Camel uses introspection to determine which method to call when
	 * delegating a message. The name of the method doesnt matter it is the argument that needs
	 * to match the type of object in the message. If there is no method with a matching argument
	 * type the message will not be delegated.
	 * 
	 * @param endpoint - endpoint where messages are expected
	 * @param delegate - {@link DbComponentEventListener} instance
	 * @return - initialized {@link RouteBuilder} instance
	 * 
	 */
	public synchronized RouteBuilder routeBuilderForIncomingRequests(final String endpoint, final DbComponentEventListener delegate, final DbComponent dbComponent) {
        return new RouteBuilder() {
          
            public void configure() {
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
  			logger.error("ErrorProcessor.process", jobid, throwable);
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
    		logger.trace(methodName, jobid, "Transport received Event. Body Type:"+exchange.getIn().getBody().getClass().getName());
//			Destination replyTo = exchange.getIn().getHeader("JMSReplyTo", Destination.class); 
//			System.out.println("... transport - value of replyTo:" + replyTo);
		}
		
	}
	
	/**
	 * Creates Camel router that will publish DbComponent state at regular intervals.
	 * 
	 * @param targetEndpointToReceiveDbComponentStateUpdate - endpoint where to publish state 
	 * @param statePublishRate - how often to publish state
	 * @return
	 * @throws Exception
	 */
	private RouteBuilder routeBuilderForDbComponentStatePost(final DbComponent dbComponent, final String targetEndpointToReceiveStateUpdate, final int statePublishRate) throws Exception {
		final DbComponentStateProcessor sp =  // an object responsible for generating the state 
			new DbComponentStateProcessor(dbComponent);
		
		return new RouteBuilder() {
		      public void configure() {
		    	String methodName = "configure";
		    	final Predicate blastGuard = new DuccBlastGuardPredicate(DbComponentConfiguration.logger);
		    	logger.trace(methodName, jobid, "timer:dbStateDumpTimer?fixedRate=true&period=" + statePublishRate);
		    	logger.trace(methodName, jobid, "endpoint=" + targetEndpointToReceiveStateUpdate);
		        from("timer:dbStateDumpTimer?fixedRate=true&period=" + statePublishRate)
              // This route uses a filter to prevent sudden bursts of messages which
              // may flood DUCC daemons causing chaos. The filter disposes any message
              // that appears in a window of 1 sec or less.
              .filter(blastGuard)
                    .process(sp)
                    .to(targetEndpointToReceiveStateUpdate);

		      }
		    };
	}
	
	/**
	 * Camel Processor responsible for generating DbComponent's state.
	 */
	private class DbComponentStateProcessor implements Processor {
		private DbComponent dbComponent;
		
		private DbComponentStateProcessor(DbComponent value) {
			dbComponent = value;
		}
		public void process(Exchange exchange) throws Exception {
			// Fetch new state from DbComponent
			DbComponentStateEvent se = dbComponent.getState();
			//	Add the state object to the Message
			exchange.getIn().setBody(se);
		}
	}
	
	/**
	 * Creates and initializes {@link DbComponent} instance. @Bean annotation identifies {@link DbComponent}
	 * as a Spring framework Bean which will be managed by Spring container.  
	 * 
	 * @return {@link DbComponent} instance
	 * 
	 * @throws Exception
	 */
	@Bean 
	public DbComponent dbServer() throws Exception {
		String location = "dbServer";
        CamelContext camelContext = common.camelContext();
		DuccEventDispatcher eventDispatcher = transportConfiguration.duccEventDispatcher(common.dbComponentStateUpdateEndpoint, camelContext);
		logger.info(location, jobid, "publish endpoint:"+common.dbComponentStateUpdateEndpoint);
		logger.info(location, jobid, "publish rate:"+common.dbComponentStatePublishRate);
		DbComponent dbComponent = new DbComponent(camelContext);
        //	Instantiate delegate listener to receive incoming messages. 
        DbComponentEventListener delegateListener = this.processManagerDelegateListener(dbComponent);
        //	Inject a dispatcher into the listener in case it needs to send
		//  a message to another component
		delegateListener.setDuccEventDispatcher(eventDispatcher);
		//	Inject Camel Router that will delegate messages to Process Manager delegate listener
		//dbComponent.getContext().addRoutes(this.routeBuilderForIncomingRequests(common.orchestratorStateUpdateEndpoint, delegateListener, dbComponent));
		dbComponent.getContext().addRoutes(this.routeBuilderForDbComponentStatePost(dbComponent, common.dbComponentStateUpdateEndpoint, Integer.parseInt(common.dbComponentStatePublishRate)));
		return dbComponent;
	}

}
