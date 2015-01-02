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
package org.apache.uima.ducc.container.jd.timeout;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.fsm.wi.IActionData;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;

public class TimeoutManager implements ITimeoutManager {

	private static Logger logger = Logger.getLogger(TimeoutManager.class, IComponent.Id.JD.name());
	
	private static TimeoutManager instance = new TimeoutManager();
	
	public static TimeoutManager getInstance() {
		return instance;
	}

	@Override
	public void pendingAck(IActionData actionData) {
		String location = "pendingAck";
		try {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

	@Override
	public void receivedAck(IActionData actionData) {
		String location = "receivedAck";
		try {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

	@Override
	public void pendingEnd(IActionData actionData) {
		String location = "pendingEnd";
		try {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

	@Override
	public void receivedEnd(IActionData actionData) {
		String location = "receivedEnd";
		try {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
}
