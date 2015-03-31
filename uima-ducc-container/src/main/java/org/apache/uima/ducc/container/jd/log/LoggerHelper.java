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
package org.apache.uima.ducc.container.jd.log;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.fsm.wi.IActionData;
import org.apache.uima.ducc.container.jd.mh.RemoteWorkerThread;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteWorkerThread;
import org.apache.uima.ducc.container.jd.wi.IWorkItem;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;

public class LoggerHelper {

	private static Logger logger = Logger.getLogger(LoggerHelper.class, IComponent.Id.JD.name());
	
	public static String getTransNo(IActionData actionData) {
		String retVal = "?";
		try {
			IMetaCasTransaction trans = actionData.getMetaCasTransaction();
			retVal = trans.getTransactionId().toString();
		}
		catch(Exception e) {
			// oh well
		}
		return retVal;
	}
	
	public static String getSeqNo(IActionData actionData) {
		String retVal = "?";
		try {
			IWorkItem wi = actionData.getWorkItem();
			retVal = getSeqNo(wi);
		}
		catch(Exception e) {
			// oh well
		}
		return retVal;
	}
	
	public static String getSeqNo(IWorkItem wi) {
		String retVal = "?";
		try {
			IMetaCas metaCas = wi.getMetaCas();
			retVal = metaCas.getSystemKey();
		}
		catch(Exception e) {
			// oh well
		}
		return retVal;
	}
	
	public static String getId(IWorkItem wi) {
		String retVal = "?";
		try {
			IMetaCas metaCas = wi.getMetaCas();
			retVal = metaCas.getUserKey();
		}
		catch(Exception e) {
			// oh well
		}
		return retVal;
	}
	
	public static String getRemote(IActionData actionData) {
		String retVal = "?";
		try {
			IMetaCasTransaction trans = actionData.getMetaCasTransaction();
			IRemoteWorkerThread rwt = new RemoteWorkerThread(trans);
			retVal = rwt.toString();
		}
		catch(Exception e) {
			// oh well
		}
		return retVal;
	}
	
	public static MessageBuffer getMessageBuffer(IActionData actionData) {
		String location = "getMessageBuffer";
		MessageBuffer mb = new MessageBuffer();
		try {
			//String transNo = getTransNo(actionData);
			String seqNo = getSeqNo(actionData);
			String remote = getRemote(actionData);
			//mb.append(Standardize.Label.transNo.get()+transNo);
			mb.append(Standardize.Label.seqNo.get()+seqNo);
			mb.append(Standardize.Label.remote.get()+remote);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return mb;
	}
	
	public static MessageBuffer getMessageBuffer(String transNo, String seqNo, String remote) {
		String location = "getMessageBuffer";
		MessageBuffer mb = new MessageBuffer();
		try {
			//mb.append(Standardize.Label.transNo.get()+transNo);
			mb.append(Standardize.Label.seqNo.get()+seqNo);
			mb.append(Standardize.Label.remote.get()+remote);
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return mb;
	}
	
	public static MessageBuffer getMessageBuffer(IMetaCasTransaction trans, IRemoteWorkerThread rwt) {
		String location = "getMessageBuffer";
		MessageBuffer mb = new MessageBuffer();
		try {
			//mb.append(Standardize.Label.transNo.get()+transNo);
			mb.append(Standardize.Label.remote.get()+rwt.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return mb;
	}
	
	public static MessageBuffer getMessageBuffer(IWorkItem wi, IRemoteWorkerThread rwt) {
		String location = "getMessageBuffer";
		MessageBuffer mb = new MessageBuffer();
		try {
			mb.append(Standardize.Label.seqNo.get()+getSeqNo(wi));
			mb.append(Standardize.Label.id.get()+getId(wi));
			mb.append(Standardize.Label.remote.get()+rwt.toString());
		}
		catch(Exception e) {
			logger.error(location, ILogger.null_id, e);
		}
		return mb;
	}
}
