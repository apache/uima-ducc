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
package org.apache.uima.ducc.container.jd.fault.injector;

import java.util.ArrayList;
import java.util.Random;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.fsm.wi.IActionData;
import org.apache.uima.ducc.container.jd.log.LoggerHelper;

public class FaultInjector {

	private static Logger logger = Logger.getLogger(FaultInjector.class, IComponent.Id.JD.name());
	
	public static boolean enabled = false;
	
	private static ArrayList<String> list = new ArrayList< String>();
	
	private enum Type { Once, Forever };
	
	private static int seed = 1;
	private static int pctFail = 25;
	private static Random random = new Random(seed);
	
	private static String getId(IActionData actionData) {
		String seqNo = LoggerHelper.getSeqNo(actionData);
		String remote = LoggerHelper.getRemote(actionData);
		String retVal = seqNo+"@"+remote;
		return retVal;
	}
	
	private static void register(IActionData actionData) {
		String id = getId(actionData);
		list.add(id);
	}
	
	private static boolean isRegistered(IActionData actionData) {
		String id = getId(actionData);
		return list.contains(id);
	}
	 
	private static boolean missing(String location, IActionData actionData, Type type) {
		boolean fault = false;
		if(enabled) {
			if(isRegistered(actionData)) {
				fault = true;
			}
			else {
				if(random.nextInt(100) < pctFail) {
					switch(type) {
					case Once:
						break;
					case Forever:
						register(actionData);
						break;
					}
					fault = true;
				}
			}
			if(fault) {
				MessageBuffer mb = LoggerHelper.getMessageBuffer(actionData);
				logger.warn(location, ILogger.null_id, mb.toString());
			}
		}
		return fault;
	}

	public static boolean missingAck(IActionData actionData) {
		String location = "missingAck";
		Type type = Type.Once;
		if(random.nextBoolean()) {
			type = Type.Forever;
		};
		return missing(location, actionData, type);
	}
	
	public static boolean missingEnd(IActionData actionData) {
		String location = "missingEnd";
		Type type = Type.Once;
		return missing(location, actionData, type);
	}
}
