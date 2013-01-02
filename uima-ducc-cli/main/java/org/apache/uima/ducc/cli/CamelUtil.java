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
package org.apache.uima.ducc.cli;

import java.util.List;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.Service;

public class CamelUtil {
	
	public static void stop(CamelContext context) throws Exception {
		List<Route> routes = context.getRoutes();
		for (Route route : routes) {
			route.getConsumer().stop();
			List<Service> services = route.getServices();
			for (Service service : services) {
				service.stop();
			}	
			route.getEndpoint().stop();
		}
		//System.out.println("Stopping AMQC");
		ActiveMQComponent amqc = (ActiveMQComponent) context.getComponent("activemq");
		amqc.stop();
		//System.out.println("Stopping AMQC - shutdown");
		amqc.shutdown();
	}	 
     
}
