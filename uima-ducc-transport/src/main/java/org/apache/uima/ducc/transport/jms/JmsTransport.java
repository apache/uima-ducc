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
package org.apache.uima.ducc.transport.jms;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;



@Configuration

@Import({CommonConfiguration.class})
public class JmsTransport {
	@Autowired CommonConfiguration common;
	//@Autowired CamelContext context;
	@Value("#{ systemProperties['ducc.broker.url'] }")String brokerUrl;
	
	@Bean
	public CamelContext jmsContext() {
		CamelContext ctx = common.camelContext();
		if ( ctx.getComponent("activemq") == null ) {
			ConnectionFactory connectionFactory =
		        new ActiveMQConnectionFactory(brokerUrl);
			JmsComponent jmsComponent = JmsComponent.jmsComponentAutoAcknowledge(connectionFactory); 
//			jmsComponent.setUseMessageIDAsCorrelationID(true);
		//  jmsComponent.setConcurrentConsumers(maxServerTasks);
//			common.camelContext().addComponent("activemq", jmsComponent);
			ctx.addComponent("activemq", jmsComponent);
		}
		
//		if ( context.getComponent("activemq") == null ) {
//	        context.addComponent("activemq", ActiveMQComponent.activeMQComponent(brokerUrl));
//		}
//		return common.camelContext();
		return ctx;
	}
	@Bean
	public CamelContext jmsContextWithClientACK() {
		CamelContext ctx = common.camelContext();
		if ( ctx.getComponent("activemq") == null ) {
			ConnectionFactory connectionFactory =
		        new ActiveMQConnectionFactory(brokerUrl);
			JmsComponent jmsComponent = JmsComponent.jmsComponentClientAcknowledge(connectionFactory); 
//			jmsComponent.setUseMessageIDAsCorrelationID(true);
		//  jmsComponent.setConcurrentConsumers(maxServerTasks);
			ctx.addComponent("activemq", jmsComponent);
		}
		
//		if ( context.getComponent("activemq") == null ) {
//	        context.addComponent("activemq", ActiveMQComponent.activeMQComponent(brokerUrl));
//		}
//		return common.camelContext();
		return ctx;
	}
	@Bean 
	public DuccEventDispatcher duccEventDispatcher() {
		return new DuccEventDispatcher(common.camelContext());
	}
}
