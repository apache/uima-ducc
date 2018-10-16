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
package org.apache.uima.ducc.ws.self.message;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

/**
 * Processor of received webserver messages to self via broker
 */
public class WebServerStateProcessor implements Processor {
	private final String brokerUrl;
	private final String username;
	private final String password;
	private final String endpoint;
	private DuccLogger duccLogger = DuccLogger.getLogger(WebServerStateProcessor.class);
	public WebServerStateProcessor(String brokerUrl, String username, String password, String endpoint) {
		this.brokerUrl = brokerUrl;
		this.username = username;
		this.password = password;
		this.endpoint = endpoint;
	}
	@Override
	public void process(Exchange exchange) throws Exception {
		try {
			ActiveMQConnectionFactory amqcf =
					new ActiveMQConnectionFactory(username, password, brokerUrl);
			amqcf.setTrustAllPackages(true);
			// this dispatcher will create a new connection for every ping.
			// No cleanup is needed as this also closes session and connection
			// after each ping.
			JmsTemplate jmsDispatcher = 
					new JmsTemplate(amqcf);
			// The endpoint is configured for Camel use and has the following
			// syntax: activemq:topic:<name>
			// Strip the activemq:topic: part to just get the name of the jms
			// topic which will be used to create ActiveMQTopic
			int pos = endpoint.indexOf("topic:")+"topic:".length();
			
			String topicName = endpoint.substring(pos);
			ActiveMQTopic topic = new ActiveMQTopic(topicName);
			jmsDispatcher.send(topic, new MessageCreator() {
				
				@Override
				public Message createMessage(Session session) throws JMSException {
					// create a ping event message
					ObjectMessage body = session.createObjectMessage();
					WebServerStateDuccEvent wse = new WebServerStateDuccEvent();
					WebServerState wss = new WebServerState();
					wse.setState(wss);
					body.setObject(wse);
					return body;
				}
			
			});
			
		} catch( Throwable t ) {
			duccLogger.error("process",null, t);
		}
//		WebServerStateDuccEvent wse = new WebServerStateDuccEvent();
//		WebServerState wss = new WebServerState();
//		wse.setState(wss);
//		exchange.getIn().setBody(wse);
	}

}
