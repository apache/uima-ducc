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
package org.apache.uima.ducc.jd;

import org.apache.uima.ducc.common.internationalization.Messages;

public class JobDriverContext {

	private static JobDriverContext instance = null;
	
	public static JobDriverContext getInstance() {
		if(instance == null) {
			instance = new JobDriverContext();
		}
		return instance;
	}
	
	private Messages systemMessages= Messages.getInstance();
	private Messages userMessages= Messages.getInstance();
	
	public void initSystemMessages(String language, String country) {
		systemMessages = Messages.getInstance(language,country);
	}
	
	public void initUserMessages(String language, String country) {
		userMessages = Messages.getInstance(language,country);
	}

	
	public Messages getSystemMessages() {
		return systemMessages;
	}
	
	public Messages getUserMessages() {
		return userMessages;
	}
}
