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
package org.apache.uima.ducc.container.common.logger;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.container.common.logger.id.Id;

public class Logger implements ILogger {
	
	private DuccLogger duccLogger = null;
	
	public static Logger getLogger(Class<?> clazz) {
		return new Logger(clazz, null);
	}
	
	public static Logger getLogger(Class<?> clazz, String component) {
		return new Logger(clazz, component);
	}
	
	private static DuccId toDuccId(Id id) {
		DuccId retVal = null;
		try {
			Long value = Long.parseLong(id.toString());
			retVal = new DuccId(value);
		}
		catch(Exception e) {
		}
		return retVal;
	}

	private Logger(Class<?> clazz, String component) {
		duccLogger = DuccLogger.getLogger(clazz, component);		
	}

	@Override
	public void fatal(String location, Id jobId, Object... args) {
		duccLogger.fatal(location, toDuccId(jobId), args);
	}

	@Override
	public void fatal(String location, Id jobId, Throwable t, Object... args) {
		duccLogger.fatal(location, toDuccId(jobId), t, args);
	}

	@Override
	public void fatal(String location, Id jobId, Id processId, Object... args) {
		duccLogger.fatal(location, toDuccId(jobId), toDuccId(processId), args);
	}

	@Override
	public void fatal(String location, Id jobId, Id processId, Throwable t, Object... args) {
		duccLogger.fatal(location, toDuccId(jobId), toDuccId(processId), t, args);
	}

	@Override
	public void debug(String location, Id jobId, Object... args) {
		duccLogger.debug(location, toDuccId(jobId), args);
	}

	@Override
	public void debug(String location, Id jobId, Throwable t, Object... args) {
		duccLogger.debug(location, toDuccId(jobId), t, args);
	}

	@Override
	public void debug(String location, Id jobId, Id processId, Object... args) {
		duccLogger.debug(location, toDuccId(jobId), toDuccId(processId), args);
	}

	@Override
	public void debug(String location, Id jobId, Id processId, Throwable t, Object... args) {
		duccLogger.debug(location, toDuccId(jobId), toDuccId(processId), t, args);
	}
	
	@Override
	public void error(String location, Id jobId, Object... args) {
		duccLogger.error(location, toDuccId(jobId), args);
	}

	@Override
	public void error(String location, Id jobId, Throwable t, Object... args) {
		duccLogger.error(location, toDuccId(jobId), t, args);
	}

	@Override
	public void error(String location, Id jobId, Id processId, Object... args) {
		duccLogger.error(location, toDuccId(jobId), toDuccId(processId), args);
	}

	@Override
	public void error(String location, Id jobId, Id processId, Throwable t, Object... args) {
		duccLogger.error(location, toDuccId(jobId), toDuccId(processId), t, args);
	}
	
	@Override
	public void info(String location, Id jobId, Object... args) {
		duccLogger.info(location, toDuccId(jobId), args);
	}

	@Override
	public void info(String location, Id jobId, Throwable t, Object... args) {
		duccLogger.info(location, toDuccId(jobId), t, args);
	}

	@Override
	public void info(String location, Id jobId, Id processId, Object... args) {
		duccLogger.info(location, toDuccId(jobId), toDuccId(processId), args);
	}

	@Override
	public void info(String location, Id jobId, Id processId, Throwable t, Object... args) {
		duccLogger.info(location, toDuccId(jobId), toDuccId(processId), t, args);
	}
	
	@Override
	public void trace(String location, Id jobId, Object... args) {
		duccLogger.trace(location, toDuccId(jobId), args);
	}

	@Override
	public void trace(String location, Id jobId, Throwable t, Object... args) {
		duccLogger.trace(location, toDuccId(jobId), t, args);
	}

	@Override
	public void trace(String location, Id jobId, Id processId, Object... args) {
		duccLogger.trace(location, toDuccId(jobId), toDuccId(processId), args);
	}

	@Override
	public void trace(String location, Id jobId, Id processId, Throwable t, Object... args) {
		duccLogger.trace(location, toDuccId(jobId), toDuccId(processId), t, args);
	}

	@Override
	public void warn(String location, Id jobId, Object... args) {
		duccLogger.warn(location, toDuccId(jobId), args);
	}

	@Override
	public void warn(String location, Id jobId, Throwable t, Object... args) {
		duccLogger.warn(location, toDuccId(jobId), t, args);
	}

	@Override
	public void warn(String location, Id jobId, Id processId, Object... args) {
		duccLogger.warn(location, toDuccId(jobId), toDuccId(processId), args);
	}

	@Override
	public void warn(String location, Id jobId, Id processId, Throwable t, Object... args) {
		duccLogger.warn(location, toDuccId(jobId), toDuccId(processId), t, args);
	}
	
	@Override
	public boolean isDebug() {
		return duccLogger.isDebug();
	}
}
