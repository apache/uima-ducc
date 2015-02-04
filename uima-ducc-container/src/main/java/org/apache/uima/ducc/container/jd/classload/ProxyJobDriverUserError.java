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
package org.apache.uima.ducc.container.jd.classload;

import java.lang.reflect.InvocationTargetException;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;
import org.apache.uima.ducc.container.jd.JobDriverException;

public class ProxyJobDriverUserError {

	private static Logger logger = Logger.getLogger(ProxyJobDriverUserError.class, IComponent.Id.JD.name());
	
	public static void loggifyUserException(Exception e) {
		String location = "loggifyUserException";
		if(e != null) {
			if(e instanceof JobDriverException) {
				Throwable t = e.getCause();
				if(t instanceof Exception) {
					Exception cause = (Exception) t;
					loggifyUserException(cause);
				}
				else {
					MessageBuffer mb = new MessageBuffer();
					mb.append(Standardize.Label.classname.get()+e.getClass().getName());
					logger.debug(location, ILogger.null_id, mb);
					logger.error(location, ILogger.null_id, e);
				}
			}
			else if(e instanceof InvocationTargetException) {
				InvocationTargetException ite = (InvocationTargetException) e;
				Throwable t = ite.getTargetException();
				if(t instanceof Exception) {
					Exception jdUserException = (Exception) t;
					String message = jdUserException.getMessage();
					loggifyUserException(message);
				}
				else {
					MessageBuffer mb = new MessageBuffer();
					mb.append(Standardize.Label.classname.get()+e.getClass().getName());
					logger.debug(location, ILogger.null_id, mb);
					logger.error(location, ILogger.null_id, e);
				}
			}
			else {
				MessageBuffer mb = new MessageBuffer();
				mb.append(Standardize.Label.classname.get()+e.getClass().getName());
				logger.debug(location, ILogger.null_id, mb);
				logger.error(location, ILogger.null_id, e);
			}
		}
		else {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.exception.get()+null);
			logger.debug(location, ILogger.null_id, mb);
			logger.error(location, ILogger.null_id, e);
		}
	}
	
	private static void loggifyUserException(String message) {
		String location = "loggify";
		if(message != null) {
			logger.error(location, ILogger.null_id, message);
		}
		else {
			MessageBuffer mb = new MessageBuffer();
			mb.append(Standardize.Label.instance.get()+null);
			logger.debug(location, ILogger.null_id, mb);
		}
	}
}
