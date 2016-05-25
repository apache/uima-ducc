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

import org.apache.uima.ducc.common.jd.files.workitem.IRemoteLocation;
import org.apache.uima.ducc.common.jd.files.workitem.RemoteLocation;
import org.apache.uima.ducc.container.common.IJdConstants.DeallocateReason;
import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverHelper;
import org.apache.uima.ducc.container.jd.cas.CasManager;
import org.apache.uima.ducc.container.jd.cas.CasManagerStats.RetryReason;
import org.apache.uima.ducc.container.jd.log.ErrorLogger;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerProcess;
import org.apache.uima.ducc.container.jd.timeout.TimeoutManager;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.jd.wi.WiTracker;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.JdState;

public class ActionHelper {
	
	protected static String getPrintable(Throwable t) {
		StringBuffer sb = new StringBuffer();
		if(t != null) {
			sb.append(t.toString());
			sb.append("\n");
			sb.append("\nAt:\n");
	        StackTraceElement[] stacktrace = t.getStackTrace();
	        for ( StackTraceElement ste : stacktrace ) {
	            sb.append("\t");
	            sb.append(ste.toString());
	            sb.append("\n");
	        }
		}
		return sb.toString();
	}
	
	protected static void toJdErrLog(String text) {
		ErrorLogger.record(text);
	}
	
	protected static void jdExhausted(Logger logger, IActionData actionData) {
		String location = "jdExhausted";
		JobDriver jd = JobDriver.getInstance();
		switch(jd.getJdState()) {
		case Ended:
			break;
		default:
			jd.advanceJdState(JdState.Ended);
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.jdState.get()+JobDriver.getInstance().getJdState());
			logger.info(location, ILogger.null_id, mb.toString());
			JobDriverHelper.getInstance().summarize();
			break;
		}
	}
	
	protected static void checkEnded(Logger logger, IActionData actionData, CasManager cm) {
		String location = "checkEnded";
		int remainder = cm.getCasManagerStats().getUnfinishedWorkCount();
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		mb.append(Standardize.Label.remainder.get()+remainder);
		logger.debug(location, ILogger.null_id, mb.toString());
		if(remainder <= 0) {
			jdExhausted(logger, actionData);
		}
	}
	
	protected static void retryWorkItem(Logger logger, IActionData actionData, CasManager cm, IMetaCas metaCas) {
		String location = "retryWorkItem";
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
		TimeoutManager.getInstance().cancelTimer(actionData);
		cm.putMetaCas(metaCas, RetryReason.UserErrorRetry);
		cm.getCasManagerStats().incEndRetry();
	}
	
	protected static void killWorkItem(Logger logger, IActionData actionData, CasManager cm) {
		String location = "killWorkItem";
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
		cm.getCasManagerStats().incEndFailure();
		checkEnded(logger, actionData, cm);
	}
	
	// The job process is killed if either the job is killed (duh) or the 
	// work item is killed, so presently this method is not needed.  Someday
	// we may allow the plug-in error handler to not kill the process so that,
	// for example, very long running work items are not unnecessarily 
	// restarted from scratch.
	
	
	protected static void killProcess(Logger logger, IActionData actionData, CasManager cm, IMetaCas metaCas, IWorkItem wi, DeallocateReason deallocateReason) {
		String location = "killProcess";
		WiTracker tracker = WiTracker.getInstance();
		IRemoteWorkerProcess rwp = tracker.getRemoteWorkerProcess(wi);
		if(rwp == null) {
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append("remote worker process not found");
			logger.info(location, ILogger.null_id, mb.toString());
		}
		else {
			String nodeIp = rwp.getNodeAddress();
			String pid = ""+rwp.getPid();
			IRemoteLocation remoteLocation = new RemoteLocation(nodeIp,pid);
			JobDriver jd = JobDriver.getInstance();
			jd.killProcess(remoteLocation, deallocateReason);
			MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
			mb.append(Standardize.Label.node.get()+nodeIp);
			mb.append(Standardize.Label.pid.get()+pid);
			logger.info(location, ILogger.null_id, mb.toString());
		}
	}
	
	protected static void killJob(Logger logger, IActionData actionData, CasManager cm) {
		String location = "killJob";
		cm.getCasManagerStats().setKillJob();
		MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
		logger.info(location, ILogger.null_id, mb.toString());
	}
}
