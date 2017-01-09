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
package org.apache.uima.ducc.ws.standardize;

public class WsStandardize {

	public enum Label {
		DUCC_WEB,
		DUCC_WEB_ENV("DUCC_WEB[env]"),
		DUCC_WEB_PROPERTY("DUCC_WEB[property]"),
		;
		
		private String text = null;
		
		Label() {
			text = name();
		}
		
		Label(String value) {
			text = value;
		}
		
		public String getText() {
			return text;
		}
		
		public String get() {
			return getText()+"=";
		}
		
	}
}
