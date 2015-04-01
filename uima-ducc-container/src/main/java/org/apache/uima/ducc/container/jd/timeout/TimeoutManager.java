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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
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
	
	private ConcurrentHashMap<IWorkItem,ITimeoutTask> mapTask = new ConcurrentHashMap<IWorkItem,ITimeoutTask>();
	private ConcurrentHashMap<IWorkItem,ScheduledFuture<?>> mapFuture = new ConcurrentHashMap<IWorkItem,ScheduledFuture<?>>();

	private TimeoutManager() {
		ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) scheduledExecutorService;
		scheduledThreadPoolExecutor.setRemoveOnCancelPolicy(true);
	}
	
	@Override
	public void pendingAck(IActionData actionData) {
		String location = "pendingAck";
		try {
			IWorkItem wi = actionData.getWorkItem();
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.Ack_Timer_Pop;
			long deadline = System.currentTimeMillis()+ackTimeout;
			ITimeoutTask timeoutTask = new TimeoutTask(fsm, event, actionData, deadline);
			register(wi, timeoutTask);
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.deadline+"+"+ackTimeout/1000);
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
			IWorkItem wi = actionData.getWorkItem();
			unregister(wi);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

	@Override
	public void pendingEnd(IActionData actionData) {
		String location = "pendingEnd";
		try {
			IWorkItem wi = actionData.getWorkItem();
			IFsm fsm = wi.getFsm();
			IEvent event = WiFsm.End_Timer_Pop;
			JobDriver jd = JobDriver.getInstance();
			long endTimeout = jd.getWorkItemTimeoutMillis();
			long deadline = System.currentTimeMillis()+endTimeout;
			ITimeoutTask timeoutTask = new TimeoutTask(fsm, event, actionData, deadline);
			register(wi, timeoutTask);
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.deadline+"+"+endTimeout/1000);
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
			IWorkItem wi = actionData.getWorkItem();
			unregister(wi);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

	@Override
	public void cancelTimer(IActionData actionData) {
		String location = "cancelTimer";
		try {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			logger.debug(location, ILogger.null_id, mb.toString());
			IWorkItem wi = actionData.getWorkItem();
			unregister(wi);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	private void register(IWorkItem wi, ITimeoutTask timeoutTask) {
		String location = "register";
		try {
			mapTask.put(wi, timeoutTask);
			Callable<?> callable = timeoutTask;
			long delay = timeoutTask.getDeadline() - System.currentTimeMillis();
			TimeUnit timeUnit = TimeUnit.MILLISECONDS;
			ScheduledFuture<?> scheduledFuture = scheduledExecutorService.schedule(callable, delay, timeUnit);
			mapFuture.put(wi, scheduledFuture);
			//
			IActionData actionData = timeoutTask.getActionData();
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.futures.get()+mapFuture.size());
			mb.append(Standardize.Label.tasks.get()+mapTask.size());
			logger.debug(location, ILogger.null_id, mb.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	private void unregister(IWorkItem wi) {
		String location = "unregister";
		try {
			ScheduledFuture<?> scheduledFuture = mapFuture.remove(wi);
			if(scheduledFuture != null) {
				scheduledFuture.cancel(false);
			}
			ITimeoutTask timeoutTask = mapTask.remove(wi);
			if(timeoutTask != null) {
				IActionData actionData = timeoutTask.getActionData();
				MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
				mb.append(Standardize.Label.futures.get()+mapFuture.size());
				mb.append(Standardize.Label.tasks.get()+mapTask.size());
				logger.debug(location, ILogger.null_id, mb.toString());
			}
			else {
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.futures.get()+mapFuture.size());
				mb.append(Standardize.Label.tasks.get()+mapTask.size());
				logger.trace(location, ILogger.null_id, mb.toString());
			}
		}	
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}

	@Override
	public void timeout(IActionData actionData) {
		String location = "timeout";
		try {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			logger.debug(location, ILogger.null_id, mb.toString());
			IWorkItem wi = actionData.getWorkItem();
			unregister(wi);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
	}
	
}
