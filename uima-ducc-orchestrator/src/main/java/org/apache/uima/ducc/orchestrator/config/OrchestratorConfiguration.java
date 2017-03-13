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
package org.apache.uima.ducc.orchestrator.config;

import org.apache.camel.Body;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jetty9.JettyHttpComponent9;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.config.DuccBlastGuardPredicate;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.IDuccLoggerComponents;
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.Orchestrator;
import org.apache.uima.ducc.orchestrator.OrchestratorComponent;
import org.apache.uima.ducc.orchestrator.OrchestratorState;
import org.apache.uima.ducc.orchestrator.event.OrchestratorEventListener;
import org.apache.uima.ducc.orchestrator.system.events.log.SystemEventsLogger;
import org.apache.uima.ducc.transport.DuccTransportConfiguration;
import org.apache.uima.ducc.transport.event.CancelJobDuccEvent;
import org.apache.uima.ducc.transport.event.CancelJobReplyDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationReplyDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceReplyDuccEvent;
import org.apache.uima.ducc.transport.event.DuccWorkReplyEvent;
import org.apache.uima.ducc.transport.event.DuccWorkRequestEvent;
import org.apache.uima.ducc.transport.event.JdReplyEvent;
import org.apache.uima.ducc.transport.event.JdRequestEvent;
import org.apache.uima.ducc.transport.event.OrchestratorStateDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobReplyDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationReplyDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceReplyDuccEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;


@Configuration
@Import({DuccTransportConfiguration.class,CommonConfiguration.class})

public class OrchestratorConfiguration {
	//	Springframework magic to inject instance of {@link CommonConfiguration}
	@Autowired CommonConfiguration common;
	//	Springframework magic to inject instance of {@link DuccTransportConfiguration}
	@Autowired DuccTransportConfiguration orchestratorTransport;

	private DuccLogger duccLogger = DuccLoggerComponents.getOrLogger(OrchestratorConfiguration.class.getName());
	private DuccId jobid = null;
	
	/**
	 * Creates Camel router that will handle incoming request messages. Each message will
	 * be unmarshalled using xstream and delegated to provided {@code OrchestratorEventListener}.
	 *   
	 * @param endpoint - endpoint where the job manager expects to receive messages
	 * @param delegate - {@code OrchestratorEventListener} instance to delegate incoming messages 
	 * @return
	 */
	
	public RouteBuilder routeBuilderForEndpoint(final String endpoint, final OrchestratorEventListener delegate) {

		return new RouteBuilder() {
			
            public void configure() {
            	from(endpoint)
            	.bean(delegate)
            	;
            }
        };
    }
	
	/**
	 * Creates Camel router that will handle incoming request messages. Each message will
	 * be unmarshalled using xstream and delegated to provided {@code OrchestratorEventListener}.
	 *   
	 * @param endpoint - endpoint where the job manager expects to receive messages
	 * @param delegate - {@code OrchestratorEventListener} instance to delegate incoming messages 
	 * @return
	 */
	/*
	public RouteBuilder routeBuilderForReplyEndpoint(final String endpoint, final OrchestratorEventListener delegate) {

		return new RouteBuilder() {
            public void configure() {
            	from(endpoint)
            	.unmarshal().xstream()
            	.process(new TransportProcessor())  // intermediate processing before delegating to event listener
            	.bean(delegate)
            	.process(new OrchestratorReplyProcessor())   // inject reply object
            	.marshal().xstream()
            	;
            }
        };
    }
	*/
  private RouteBuilder routeBuilder(final CamelContext context, final OrchestratorEventListener delegate) throws Exception {
    
    return new RouteBuilder() {
          public void configure() {
            
            JettyHttpComponent9 jettyComponent = new JettyHttpComponent9();
            
			//ExchangeMonitor xmError = new ExchangeMonitor(LifeStatus.Error, ExchangeType.Receive);
			
            context.addComponent("jetty", jettyComponent);
            onException(Throwable.class).maximumRedeliveries(0).handled(false).process(new ErrorProcessor());
            
            from("jetty://http://0.0.0.0:"+common.duccORHttpPort+"/or")
            .unmarshal().xstream()
            
            .bean(delegate)
            .process(new OrchestratorReplyProcessor())   // inject reply object
            .process(new Processor() {
              
              public void process(Exchange exchange) throws Exception {
                exchange.getOut().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
                exchange.getOut().setHeader("content-type", "text/xml");
                Object o = exchange.getIn().getBody();
                if ( o != null ) {
                  String body = XStreamUtils.marshall(o);
                  exchange.getOut().setBody(body);
                  exchange.getOut().setHeader("content-length", body.length());
                } else {
                  duccLogger.warn("RouteBuilder.configure", null, new DuccRuntimeException("Orchestrator Has Not Provided a Reply Object."));
                  exchange.getOut().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
                } 
              }
            })
            ;
          }
        };
  }
  
	private class OrchestratorReplyProcessor implements Processor {
		
		private OrchestratorReplyProcessor() {
		}
		
		public void process(Exchange exchange) throws Exception {
			Object obj = exchange.getIn().getBody();
			if(obj instanceof JdRequestEvent) {
				JdRequestEvent jdRequestEvent = exchange.getIn().getBody(JdRequestEvent.class);
				JdReplyEvent jdReplyEvent = new JdReplyEvent();
				jdReplyEvent.setProcessMap(jdRequestEvent.getProcessMap());
				String killDriverReason = jdRequestEvent.getKillDriverReason();
				jdReplyEvent.setKillDriverReason(killDriverReason);
				exchange.getIn().setBody(jdReplyEvent);
			}
			if(obj instanceof DuccWorkRequestEvent) {
				DuccWorkRequestEvent duccWorkRequestEvent = exchange.getIn().getBody(DuccWorkRequestEvent.class);
				DuccWorkReplyEvent duccWorkReplyEvent = new DuccWorkReplyEvent();
				duccWorkReplyEvent.setDw(duccWorkRequestEvent.getDw());
				exchange.getIn().setBody(duccWorkReplyEvent);
			}
			if(obj instanceof SubmitJobDuccEvent) {
				SubmitJobDuccEvent submitJobEvent = exchange.getIn().getBody(SubmitJobDuccEvent.class);
				SubmitJobReplyDuccEvent replyJobEvent = new SubmitJobReplyDuccEvent();
				replyJobEvent.setProperties(submitJobEvent.getProperties());
				exchange.getIn().setBody(replyJobEvent);
				SystemEventsLogger.info(IDuccLoggerComponents.abbrv_orchestrator, submitJobEvent, replyJobEvent);
			}
			if(obj instanceof CancelJobDuccEvent) {
				CancelJobDuccEvent cancelJobEvent = exchange.getIn().getBody(CancelJobDuccEvent.class);
				CancelJobReplyDuccEvent replyJobEvent = new CancelJobReplyDuccEvent();
				replyJobEvent.setProperties(cancelJobEvent.getProperties());
				exchange.getIn().setBody(replyJobEvent);
				SystemEventsLogger.info(IDuccLoggerComponents.abbrv_orchestrator, cancelJobEvent, replyJobEvent);
			}
			if(obj instanceof SubmitReservationDuccEvent) {
				SubmitReservationDuccEvent submitReservationEvent = exchange.getIn().getBody(SubmitReservationDuccEvent.class);
				SubmitReservationReplyDuccEvent replyReservationEvent = new SubmitReservationReplyDuccEvent();
				replyReservationEvent.setProperties(submitReservationEvent.getProperties());
				exchange.getIn().setBody(replyReservationEvent);
				SystemEventsLogger.info(IDuccLoggerComponents.abbrv_orchestrator, submitReservationEvent, replyReservationEvent);
			}
			if(obj instanceof CancelReservationDuccEvent) {
				CancelReservationDuccEvent cancelReservationEvent = exchange.getIn().getBody(CancelReservationDuccEvent.class);
				CancelReservationReplyDuccEvent replyReservationEvent = new CancelReservationReplyDuccEvent();
				replyReservationEvent.setProperties(cancelReservationEvent.getProperties());
				exchange.getIn().setBody(replyReservationEvent);
				SystemEventsLogger.info(IDuccLoggerComponents.abbrv_orchestrator, cancelReservationEvent, replyReservationEvent);
			}
			if(obj instanceof SubmitServiceDuccEvent) {
				SubmitServiceDuccEvent submitServiceEvent = exchange.getIn().getBody(SubmitServiceDuccEvent.class);
				SubmitServiceReplyDuccEvent replyServiceEvent = new SubmitServiceReplyDuccEvent();
				replyServiceEvent.setProperties(submitServiceEvent.getProperties());
				exchange.getIn().setBody(replyServiceEvent);
				SystemEventsLogger.info(IDuccLoggerComponents.abbrv_orchestrator, submitServiceEvent, replyServiceEvent);
			}
			if(obj instanceof CancelServiceDuccEvent) {
				CancelServiceDuccEvent cancelServiceEvent = exchange.getIn().getBody(CancelServiceDuccEvent.class);
				CancelServiceReplyDuccEvent replyServiceEvent = new CancelServiceReplyDuccEvent();
				replyServiceEvent.setProperties(cancelServiceEvent.getProperties());
				exchange.getIn().setBody(replyServiceEvent);
				SystemEventsLogger.info(IDuccLoggerComponents.abbrv_orchestrator, cancelServiceEvent, replyServiceEvent);
			}
		}
	}
	
	/**
	 * Creates Camel router that will publish Orchestrator state at regular intervals.
	 * 
	 * @param targetEndpointToReceiveOrchestratorStateUpdate - endpoint where to publish JM state 
	 * @param statePublishRate - how often to publish state
	 * @return
	 * @throws Exception
	 */
	private RouteBuilder routeBuilderForOrchestratorStatePost(final Orchestrator orchestrator, final String targetEndpointToReceiveOrchestratorStateUpdate, final int statePublishRate) throws Exception {
		final OrchestratorStateProcessor orchestratorp =  // an object responsible for generating the state 
			new OrchestratorStateProcessor(orchestrator);
		
		return new RouteBuilder() {
		      public void configure() {		            
		    	
		    	final Predicate blastFilter = new DuccBlastGuardPredicate(duccLogger);
		    	
		        from("timer:orchestratorStateDumpTimer?fixedRate=true&period=" + statePublishRate)
		              // This route uses a filter to prevent sudden bursts of messages which
		        	  // may flood DUCC daemons causing chaos. The filter disposes any event
		        	  // that appears in a window of 1 sec or less.
		        	  .filter(blastFilter)	
		              //.process(xmStart)
		        	  .process(orchestratorp)
		        	  //.process(xmEnded)
		        	  .to(targetEndpointToReceiveOrchestratorStateUpdate)
		        	  ;
		      }
		    };
	}
	
	/**
	 * Camel Processor responsible for generating Orchestrator's state.
	 * 
	 */
	private class OrchestratorStateProcessor implements Processor {
		private Orchestrator orchestrator;
		
		private OrchestratorStateProcessor(Orchestrator orchestrator) {
			this.orchestrator = orchestrator;
		}
		public void process(Exchange exchange) throws Exception {
			String location = "OrchestratorStateProcessor.process";
			// Fetch new state from Orchestrator
			OrchestratorStateDuccEvent jse = orchestrator.getState();
			//	add sequence number to the outgoing message. This should be used to manage
			//  processing order in the consumer
			OrchestratorState orchestratorState = OrchestratorState.getInstance();
			long seqNo = orchestratorState.getNextSequenceNumberState();
			duccLogger.debug(location, jobid, ""+seqNo);
			jse.setSequence(seqNo);
			//	Add the state object to the Message
			exchange.getIn().setBody(jse);
		}
	}
	
	/**
	 * Instantiate a listener to which Camel will route a body of the incoming message.
	 * The listener should provide a method for each object class it expects to receive.
	 * Camel uses introspection to analyze given listener and find a match based on
	 * what is in the incoming message. 
	 * 
	 * @return
	 */
	public OrchestratorEventListener orchestratorDelegateListener(OrchestratorComponent orchestrator) {
		OrchestratorEventListener orchestratorel =  new OrchestratorEventListener(orchestrator);
		return orchestratorel;
	}

	@Bean 
	public OrchestratorComponent orchestrator() throws Exception {
		OrchestratorComponent orchestrator = new OrchestratorComponent(common.camelContext());
        //	Instantiate JobManagerEventListener delegate listener. This listener will receive
        //	incoming messages. 
        OrchestratorEventListener delegateListener = this.orchestratorDelegateListener(orchestrator);
		//	Inject a dispatcher into the listener in case it needs to send
		//  a message to another component
		delegateListener.setDuccEventDispatcher(orchestratorTransport.duccEventDispatcher(common.pmRequestEndpoint, orchestrator.getContext()));
//		orchestrator.getContext().addRoutes(this.routeBuilderForReplyEndpoint(common.orchestratorRequestEndpoint, delegateListener));
    orchestrator.getContext().addRoutes(this.routeBuilder(orchestrator.getContext(), delegateListener));
		orchestrator.getContext().addRoutes(this.routeBuilderForEndpoint(common.rmStateUpdateEndpoint, delegateListener));
		orchestrator.getContext().addRoutes(this.routeBuilderForEndpoint(common.smStateUpdateEndpoint, delegateListener));
		orchestrator.getContext().addRoutes(this.routeBuilderForEndpoint(common.jdStateUpdateEndpoint,delegateListener));
		orchestrator.getContext().addRoutes(this.routeBuilderForEndpoint(common.nodeInventoryEndpoint,delegateListener));
		orchestrator.getContext().addRoutes(this.routeBuilderForOrchestratorStatePost(orchestrator, common.orchestratorStateUpdateEndpoint, Integer.parseInt(common.orchestratorStatePublishRate)));
		return orchestrator;
	}
  public class ErrorProcessor implements Processor {

    public void process(Exchange exchange) throws Exception {
      // the caused by exception is stored in a property on the exchange
      Throwable caused = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
      duccLogger.error("ErrorProcessor.process",null, caused);
      exchange.getOut().setBody(caused); //XStreamUtils.marshall(caused));
    }
  }
  public class ServiceRequestHandler {
    public void handleRequest(@Body SubmitJobDuccEvent jobSubmit) throws Exception {
 //   public void handleRequest(@Body ErrorProcessor jobSubmit) throws Exception {
      System.out.println("ServiceRequestHandler Received Request of type: "+jobSubmit.getClass().getName());
       synchronized(this) {
         this.wait(2000);
       }
    }
  }
}
