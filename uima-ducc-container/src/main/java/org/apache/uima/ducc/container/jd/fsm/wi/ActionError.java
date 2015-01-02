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
package org.apache.uima.ducc.container.jd.fsm.wi;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;

public class ActionError implements IAction {

	private static Logger logger = Logger.getLogger(ActionError.class, IComponent.Id.JD.name());
	
	@Override
	public String getName() {
		return ActionError.class.getName();
	}

	@Override
	public void engage(Object objectData) {
		String location = "engage";
		logger.trace(location, ILogger.null_id, "");
		IActionData actionData = (IActionData) objectData;
		//TODO
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
	}
}
