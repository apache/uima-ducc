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
package org.apache.uima.ducc.transport;

import java.util.List;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.authentication.BrokerCredentials;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;


@Configuration
@Scope("prototype")
public class DuccTransportConfiguration {
	@Value("#{ systemProperties['ducc.broker.url'] }")String brokerUrl;
	private static ActiveMQComponent duccAMQComponent = null;
	
	@Value("#{ systemProperties['ducc.broker.credentials.file'] }")String brokerCredentialsFile;

	public void configureJMSTransport(DuccLogger logger, String endpoint, CamelContext context) throws Exception {
		BrokerCredentials.Credentials credentials = null;
	  synchronized(ActiveMQComponent.class) {
	    if ( duccAMQComponent == null ) {
	      duccAMQComponent = new ActiveMQComponent(context);
			duccAMQComponent.setBrokerURL(brokerUrl);
			logger.info("configureJMSTransport", null, "Broker URL: "+brokerUrl);
	      //logger.info("configureJMSTransport", null, "brokerCredentialsFile:"+brokerCredentialsFile);
	      if ( brokerCredentialsFile != null ) {
	    	  String path = Utils.resolvePlaceholderIfExists(brokerCredentialsFile, System.getProperties());
		      //logger.info("configureJMSTransport", null, "brokerCredentialsFile Path:"+path);
	    	  credentials = BrokerCredentials.get(path);
		      //logger.info("configureJMSTransport", null, "Username:"+credentials.getUsername()+" Password:"+credentials.getPassword());
				if ( credentials.getUsername() != null && credentials.getPassword() != null ) {
					duccAMQComponent.setUserName(credentials.getUsername());
				    duccAMQComponent.setPassword(credentials.getPassword());
				    System.out.println(">>>>>>>>>>>>>>> Running with AMQ Credentials");
				    logger.info("configureJMSTransport", null, ">>>>>>>>>>>>>>> Running with AMQ Credentials");
				} 
	      }
	      List<String> cs =context.getComponentNames();
	      for( String s : cs ) {
	    	  logger.info("configureJMSTransport", null, "Componennt:"+s);
	      }
	      context.addComponent("activemq",duccAMQComponent);
	    }
	  }
	  whiteListAllPkgs();
	}
	public void configureJMSTransport( String endpoint, CamelContext context) throws Exception {
		BrokerCredentials.Credentials credentials = null;
	  synchronized(ActiveMQComponent.class) {
	    if ( duccAMQComponent == null ) {
		      duccAMQComponent = new ActiveMQComponent(context);
		      duccAMQComponent.setBrokerURL(brokerUrl);

//	      duccAMQComponent = ActiveMQComponent.activeMQComponent(brokerUrl);
	      if ( brokerCredentialsFile != null ) {
	    	  String path = Utils.resolvePlaceholderIfExists(brokerCredentialsFile, System.getProperties());
	    	  credentials = BrokerCredentials.get(path);
				if ( credentials.getUsername() != null && credentials.getPassword() != null ) {
					duccAMQComponent.setUserName(credentials.getUsername());
				    duccAMQComponent.setPassword(credentials.getPassword());
				    System.out.println(">>>>>>>>>>>>>>> Running with AMQ Credentials");
				} 
	      }
	      
	      context.addComponent("activemq",duccAMQComponent);
	    }
	  }
	  whiteListAllPkgs();
	}
	public DuccEventDispatcher duccEventDispatcher(DuccLogger logger,String requestEndpoint,CamelContext context) throws Exception {
    configureJMSTransport(logger, requestEndpoint, context);
		return new DuccEventDispatcher(context, requestEndpoint);
	}
	public DuccEventDispatcher duccEventDispatcher(String requestEndpoint,CamelContext context) throws Exception {
	    configureJMSTransport(requestEndpoint, context);
			return new DuccEventDispatcher(context, requestEndpoint);
	}
    private void whiteListAllPkgs() {
        System.out.println("Getting AMQ Factory");
      PooledConnectionFactory amqf = (PooledConnectionFactory)duccAMQComponent.getConfiguration().getConnectionFactory();
      ActiveMQConnectionFactory f = (ActiveMQConnectionFactory)amqf.getConnectionFactory();

   f.setTrustAllPackages(true);
//      logger.info("Component",null,"Whitelisted All Packages For AMQ Exchanges");                                                                           
  System.out.println("White Listed Packages for AMQ Exchanges");


}

}

