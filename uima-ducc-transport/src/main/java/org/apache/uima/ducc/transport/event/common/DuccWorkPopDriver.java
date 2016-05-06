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
package org.apache.uima.ducc.transport.event.common;

public class DuccWorkPopDriver extends DuccWorkPop {

    /**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;
		private String serverUri = null;
		private String endPoint = null;
		private String cr = null;
		private String crConfig = null;
		private String metaTimeout = null;
		private String lostTimeout = null;
		private String wiTimeout = null;
		private String processExceptionHandler = null;
		
		// This constructor no longer needed
		public DuccWorkPopDriver(String serverUri, String endPoint, String cr, String crConfig, String metaTimeout, String lostTimeout, String wiTimeout, String processExceptionHandler) {
			this.serverUri = serverUri;
			this.endPoint = endPoint;
			this.cr = cr;
			this.crConfig = crConfig;
			this.metaTimeout = metaTimeout;
			this.lostTimeout = lostTimeout;
			this.wiTimeout = wiTimeout;
			this.processExceptionHandler = processExceptionHandler;
		}
		
		public DuccWorkPopDriver() {
		}

		public String getServerUri() {
			return serverUri;
		}
		
		public String getEndPoint() {
			return endPoint;
		}
		
		public String getCR() {
			return cr;
		}
		
		public String getCRConfig() {
			return crConfig;
		}
		
		public String getMetaTimeout() {
			return metaTimeout;
		}
		
		public String getLostTimeout() {
			return lostTimeout;
		}
		
		public String getWiTimeout() {
			return wiTimeout;
		}
		
		public String getProcessExceptionHandler() {
			return processExceptionHandler;
		}
}
