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
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.fsm.wi.IActionData;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;

public class TimeoutTask implements ITimeoutTask {

	private static Logger logger = Logger.getLogger(TimeoutTask.class, IComponent.Id.JD.name());
	
	private IFsm fsm = null;
	private IEvent event = null;
	private IActionData actionData = null;
	private long deadline = 0;
	
	public TimeoutTask(IFsm fsm, IEvent event, IActionData actionData, long deadline) {
		setFsm(fsm);
		setEvent(event);
		setActionData(actionData);
		setDeadline(deadline);
	}

	private void setFsm(IFsm value) {
		fsm = value;
	}
	
	@Override
	public IFsm getFsm() {
		return fsm;
	}
	
	private void setEvent(IEvent value) {
		event = value;
	}
	
	@Override
	public IEvent getEvent() {
		return event;
	}
	
	private void setActionData(IActionData value) {
		actionData = value;
	}
	
	@Override
	public IActionData getActionData() {
		return actionData;
	}
	
	private void setDeadline(long value) {
		deadline = value;
	}
	
	@Override
	public long getDeadline() {
		return deadline;
	}
	
	@Override
	public Object call() throws Exception {
		String location = "call";
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		mb.append(Standardize.Label.event.get()+event.getName());
		logger.warn(location, ILogger.null_id, mb.toString());
		TimeoutManager.getInstance().timeout(actionData);
		fsm.transition(event, actionData);
		return null;
	}

}
