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
package org.apache.uima.ducc.common.authentication;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;

public class BrokerCredentials {
	public static Credentials get(String brokerCredentialsFile) throws FileNotFoundException{
		Credentials cr = new Credentials();
		Properties properties = new Properties();
		if ( brokerCredentialsFile != null ) {
			try {
			    if (!(new File(brokerCredentialsFile)).canRead()) {
			        DuccLogger logger = DuccLogger.getLogger(BrokerCredentials.class.getName(), null);
			        String component = System.getProperty("ducc.deploy.components");
			        if ( component != null ) {
			        	if ( !"uima-as".equals(component) &&
			        		 !"jd".equals(component) &&
			        		 !"service".equals(component) &&
			        		 !"job-process".equals(component) ) {
					        // Default of no name & password => anonymous access
					        logger.info("BrokerCredentials.get", null, "Cannot access broker credentials file so will have restricted access");
			        	}
			        }
			        return cr;
			    }
				properties.load(new FileInputStream(brokerCredentialsFile));
						//Utils.findDuccHome()+File.separator+"activemq"+File.separator+"credentials.properties"));
				cr.setUsername(properties.getProperty("ducc.broker.admin.username"));
				cr.setPassword(properties.getProperty("ducc.broker.admin.password"));
				return cr;

			} catch( IOException e) {}
		} 
		// credentials not available
		throw new FileNotFoundException();
	}
	public static class Credentials {
		String username;
		String password;
		public String getUsername() {
			return username;
		}
		public void setUsername(String username) {
			this.username = username;
		}
		public String getPassword() {
			return password;
		}
		public void setPassword(String password) {
			this.password = password;
		}
		
	}
}
