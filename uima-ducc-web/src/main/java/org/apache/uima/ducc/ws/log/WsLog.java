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
package org.apache.uima.ducc.ws.log;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;

// Simplified by passing the logger instead of the class name as the 1st arg

public class WsLog {

	private static DuccId duccId = null;
	
	public static void enter(DuccLogger logger, String mName) {
		logger.trace(mName, duccId, "enter");
	}
	
	public static void exit(DuccLogger logger, String mName) {
		logger.trace(mName, duccId, "exit");
	}
	
	public static void info(DuccLogger logger, String mName, String text) {
		logger.info(mName, duccId, text);
	}
	
	public static void info(DuccLogger logger, String mName, DuccId duccId, String text) {
		logger.info(mName, duccId, text);
	}
	
	public static void debug(DuccLogger logger, String mName, DuccId duccid, String text) {
		logger.debug(mName, duccid, text);
	}
	
	public static void debug(DuccLogger logger, String mName, String text) {
		logger.debug(mName, duccId, text);
	}
	
	public static void debug(DuccLogger logger, String mName, Exception e) {
		logger.debug(mName, duccId, e);
	}
	
	public static void debug(DuccLogger logger, String mName, Throwable t) {
		logger.debug(mName, duccId, t);
	}
	
	public static void trace(DuccLogger logger, String mName, String text) {
		logger.trace(mName, duccId, text);
	}
	
	public static void trace(DuccLogger logger, String mName, Exception e) {
		logger.trace(mName, duccId, e);
	}
	
	public static void trace(DuccLogger logger, String mName, Throwable t) {
		logger.trace(mName, duccId, t);
	}
	
	public static void error(DuccLogger logger, String mName, Exception e) {
		logger.error(mName, duccId, e);
	}
	
	public static void error(DuccLogger logger, String mName, Throwable t) {
		logger.error(mName, duccId, t);
	}
	
	public static void error(DuccLogger logger, String mName, String text) {
		logger.error(mName, duccId, text);
	}
	
	public static void warn(DuccLogger logger, String mName, String text) {
		logger.warn(mName, duccId, text);
	}
}


