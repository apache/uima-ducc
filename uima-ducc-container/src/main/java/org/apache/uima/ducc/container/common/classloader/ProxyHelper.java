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
package org.apache.uima.ducc.container.common.classloader;

import java.lang.reflect.InvocationTargetException;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;

public class ProxyHelper {

	public static Exception getTargetException(Exception e) {
		Exception x = e;
		if(e != null) {
			if(e instanceof InvocationTargetException) {
				InvocationTargetException i = (InvocationTargetException) e;
				Throwable t = i.getTargetException();
				if(t instanceof Exception) {
					x = (Exception) t;
				}
			}
		}
		return x;
	}
	
	public static void loggifyUserException(Logger logger, Exception e) {
		String location = "loggifyUserException";
		if(e != null) {
			if(e instanceof ProxyException) {
				Throwable t = e.getCause();
				if(t instanceof Exception) {
					Exception cause = (Exception) t;
					loggifyUserException(logger, cause);
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
}
