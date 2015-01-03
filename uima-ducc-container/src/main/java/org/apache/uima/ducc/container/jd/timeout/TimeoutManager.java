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

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.fsm.wi.IActionData;
import org.apache.uima.ducc.container.jd.fsm.wi.WiFsm;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;

public class TimeoutManager implements ITimeoutManager {

	private static Logger logger = Logger.getLogger(TimeoutManager.class, IComponent.Id.JD.name());
	
	private static TimeoutManager instance = new TimeoutManager();
	
	public static TimeoutManager getInstance() {
		return instance;
	}
	
	private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
	
	private long ackTimeout= 60*1000;
	
	private ConcurrentHashMap<IFsm,ITimeoutTask> mapTask = new ConcurrentHashMap<IFsm,ITimeoutTask>();
	private ConcurrentHashMap<IFsm,ScheduledFuture<?>> mapFuture = new ConcurrentHashMap<IFsm,ScheduledFuture<?>>();

	@Override
	public void pendingAck(IActionData actionData) {
		String location = "pendingAck";
		try {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			logger.debug(location, ILogger.null_id, mb.toString());
			IWorkItem wi = actionData.getWorkItem();
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.Ack_Timer_Pop;
			long deadline = System.currentTimeMillis()+ackTimeout;
			ITimeoutTask timeoutTask = new TimeoutTask(fsm, event, actionData, deadline);
			register(fsm, timeoutTask);
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

	private boolean wip = true;
	
	public void register(IFsm fsm, ITimeoutTask timeoutTask) {
		String location = "register";
		if(wip) {
			return;
		}
		try {
			mapTask.put(fsm, timeoutTask);
			Callable<?> callable = timeoutTask;
			long delay = timeoutTask.getDeadline() - System.currentTimeMillis();
			TimeUnit timeUnit = TimeUnit.MILLISECONDS;
			ScheduledFuture<?> scheduledFuture = scheduledExecutorService.schedule(callable, delay, timeUnit);
			mapFuture.put(fsm, scheduledFuture);
			//
			IActionData actionData = timeoutTask.getActionData();
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			logger.info(location, ILogger.null_id, mb.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	public void unregister(IFsm fsm) {
		String location = "unregister";
		if(wip) {
			return;
		}
		try {
			ScheduledFuture<?> scheduledFuture = mapFuture.remove(fsm);
			if(scheduledFuture != null) {
				scheduledFuture.cancel(false);
			}
			ITimeoutTask timeoutTask = mapTask.remove(fsm);
			if(timeoutTask != null) {
				IActionData actionData = timeoutTask.getActionData();
				MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
				logger.info(location, ILogger.null_id, mb.toString());
			}
		}	
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
}
