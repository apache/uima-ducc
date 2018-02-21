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
package org.apache.uima.ducc.transport.dispatcher;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RuntimeExchangeException;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.DuccExchange;
import org.apache.uima.ducc.transport.event.DuccEvent;

public class DuccEventDispatcher {
	
  private static DuccLogger logger = new DuccLogger(DuccEventDispatcher.class);
  private static DuccId jobid = null;
  
  private ProducerTemplate pt;

  private String targetEndpoint;

  protected CamelContext context;

  public DuccEventDispatcher(CamelContext context) {
    this.pt = context.createProducerTemplate();
  }

  public DuccEventDispatcher(CamelContext context, String targetEndpoint) {
    this.pt = context.createProducerTemplate();
    this.context = context;
    this.targetEndpoint = targetEndpoint;
  }
  
  public void dispatch(int serviceSocketPort, DuccEvent duccEvent) throws Exception {
    //  by default Mina doesnt include exchange.The transferExchange=true forces inclusion of the Exchange in 
    //  a message
    //  Dispatch event via a socket (Mina Camel Component) to a service running on the 
    //  same machine 
//    pt.sendBody("mina:tcp://localhost:"+serviceSocketPort+"?transferExchange=true&sync=false", marshallDuccEvent(duccEvent));
    pt.sendBody("mina:tcp://localhost:"+serviceSocketPort+"?transferExchange=true&sync=false", duccEvent);  }

  public void dispatch(String targetEndpoint, DuccEvent duccEvent) throws Exception {
    dispatch(targetEndpoint, duccEvent, null);
  }

  public void dispatch(DuccEvent duccEvent, String nodeList) throws Exception {
    dispatch(this.targetEndpoint, duccEvent, nodeList);
  }

  public void dispatch(String endpoint, DuccEvent duccEvent, String nodeList) throws Exception {
	  String location = "dispatch";
	  try {
      if (nodeList != null) {
        // No reply is expected
        pt.sendBodyAndHeader(endpoint, duccEvent, DuccExchange.TARGET_NODES_HEADER_NAME, nodeList);
      } else {
        pt.asyncRequestBody(endpoint, duccEvent);
      }
    } catch (Exception e) {
    	logger.error(location, jobid, e);
    }
  }

  public void dispatch(String serializedEvent) throws Exception {
    // this is a one-way send. Reply is not expected
    pt.sendBody(targetEndpoint, serializedEvent);
  }

  public void dispatch(DuccEvent duccEvent) throws Exception {
    pt.asyncSendBody(targetEndpoint, duccEvent);
  }

  public void dispatch(DuccEvent duccEvent, final Map<String, Object> headers) throws Exception {
    pt.sendBodyAndHeaders(targetEndpoint, duccEvent, headers);
  }
  public void dispatch(DuccEvent duccEvent, String serviceEndpoint, final Map<String, Object> headers) throws Exception {
    //  by default Mina doesnt include Exchange message envelope containing headers.The 
    //  transferExchange=true forces inclusion of the Exchange in a message.
    //  Dispatch event via a socket (Mina Camel Component) to a service running on the 
    //  same machine 
//    pt.sendBodyAndHeaders("mina:tcp://localhost:"+serviceSocketPort+"?transferExchange=true&sync=false", duccEvent, headers);
    pt.sendBodyAndHeaders(serviceEndpoint, duccEvent, headers);
  }

  public DuccEvent dispatchAndWaitForDuccReply(DuccEvent duccEvent) throws Exception {
    int maxRetryCount = 20;
    int i = 0;
    Object response = null;
    RuntimeExchangeException ree = null;

    // retry up to 20 times. This is an attempt to handle an error thrown
    // by Camel: Failed to resolve replyTo destination on the exchange
    // Camel waits at most 10000ms( 10secs) for AMQ to create a temp queue.
    // After 10secs Camel times out and throws an Exception.
    for (; i < maxRetryCount; i++) {
      try {
        response = pt.sendBody(targetEndpoint, ExchangePattern.InOut, duccEvent);
        ree = null; // all is well - got a reply
        break; // done here

      } catch (RuntimeExchangeException e) {
        String msg = e.getMessage();
        // Only retry if AMQ failed to create a temp queue
        if (msg != null && msg.startsWith("Failed to resolve replyTo destination on the exchange")) {
          ree = e;
        } else {
          throw new DuccRuntimeException("Ducc JMS Dispatcher is unable to deliver a request.", e);
        }
      }
    }
    // when retries hit the threshold, just throw an exception
    if (i == maxRetryCount) {
      throw new DuccRuntimeException(
              "ActiveMQ failed to create temp reply queue. After 20 attempts to deliver request to the OR, Ducc JMS Dispatcher is giving up.",
              ree);
    }
    DuccEvent reply = (DuccEvent) response;
    return reply;
  }

  public void stop() throws Exception {
    if (pt != null) {
      pt.stop();
    }
  }
}
