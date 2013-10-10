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

import java.io.FileNotFoundException;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.uima.ducc.common.authentication.BrokerCredentials;
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
	
	@Value("#{ systemProperties['ducc.broker.credentials.file'] }")String brokerCredentialsFile;

	private ConnectionFactory getConnectionFactory() {
		BrokerCredentials.Credentials credentials = null;
		ConnectionFactory connectionFactory;
		try {
			credentials = BrokerCredentials.get(brokerCredentialsFile);
			if ( credentials.getUsername() != null && credentials.getPassword() != null ) {
				connectionFactory =
				        new ActiveMQConnectionFactory(credentials.getUsername(), credentials.getPassword(), brokerUrl);
			} else {
				connectionFactory =
				        new ActiveMQConnectionFactory(brokerUrl);
			}
		} catch( FileNotFoundException fne) {
			connectionFactory =
			        new ActiveMQConnectionFactory(brokerUrl);
		}
		return connectionFactory;
	}
	@Bean
	public CamelContext jmsContext() {
		CamelContext ctx = common.camelContext();
		if ( ctx.getComponent("activemq") == null ) {
			ConnectionFactory connectionFactory = getConnectionFactory();
			JmsComponent jmsComponent = JmsComponent.jmsComponentAutoAcknowledge(connectionFactory); 
			ctx.addComponent("activemq", jmsComponent);
		}
		return ctx;
	}
 	@Bean
	public CamelContext jmsContextWithClientACK() {
		CamelContext ctx = common.camelContext();
		if ( ctx.getComponent("activemq") == null ) {
			ConnectionFactory connectionFactory = getConnectionFactory();
			JmsComponent jmsComponent = JmsComponent.jmsComponentClientAcknowledge(connectionFactory); 
			ctx.addComponent("activemq", jmsComponent);
		}
		
		return ctx;
	}
	@Bean 
	public DuccEventDispatcher duccEventDispatcher() {
		return new DuccEventDispatcher(common.camelContext());
	}

	
}
