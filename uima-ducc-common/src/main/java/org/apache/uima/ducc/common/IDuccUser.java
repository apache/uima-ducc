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
package org.apache.uima.ducc.common;

// Note: Coordinate with org.apache.uima.ducc.IUser

public interface IDuccUser {
	
	public enum EnvironmentVariable {
		DUCC_HOME("DUCC_HOME"),
		DUCC_ID_JOB("DUCC_JOBID"),
		DUCC_ID_PROCESS("DUCC_PROCESSID"),
		DUCC_ID_SERVICE("DUCC_SERVICE_INSTANCE"),
		DUCC_LOG_PREFIX("DUCC_PROCESS_LOG_PREFIX"),
		//
		DUCC_IP("DUCC_IP"),
		DUCC_NODENAME("DUCC_NODENAME"),
		//
		DUCC_USER_CP_PREPEND("DUCC_USER_CP_PREPEND"),
		//
		USER("USER"),
		;
		
		private String value = null;
		
		private EnvironmentVariable(String valueForKey) {
			setValue(valueForKey);
		}
		
		private void setValue(String valueForKey) {
			value = valueForKey;
		}
		
		private String getValue() {
			return value;
		}
		
		public String standard() {
			return name();
		}
		
		public String mapped() {
			return getValue();
		}
		
		public String value() {
			String retVal = mapped();
			switch(this) {
			default:
				break;
			}
			return retVal;
		}
	}
	
	public enum DashD {
		DUCC_ID_PROCESS_UNIQUE("ducc.deploy.JpUniqueId"),
		;
		
		private String value = null;
		
		private DashD(String valueForKey) {
			setValue(valueForKey);
		}
		
		private void setValue(String valueForKey) {
			value = valueForKey;
		}
		
		private String getValue() {
			return value;
		}
		
		public String standard() {
			return name();
		}
		
		public String mapped() {
			return getValue();
		}
		
		public String value() {
			String retVal = mapped();
			switch(this) {
			default:
				break;
			}
			return retVal;
		}
		
		public String dvalue() {
			return "-D"+value();
		}
	}
	
}
