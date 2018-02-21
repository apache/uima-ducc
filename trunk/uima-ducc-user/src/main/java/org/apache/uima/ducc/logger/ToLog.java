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
package org.apache.uima.ducc.logger;

import java.io.File;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is used to record messages into file ErrorHandler.log
 * in the user's Job log directory.
 */

public class ToLog {
	
	private static String getFilePath(Class<?> clazz) {
		String filePath = null;
		String key = "ducc.process.log.dir";
		String value = System.getProperty(key);
		if(value != null) {
			filePath = value;
			if(!filePath.endsWith(File.separator)) {
				filePath = filePath+File.separator;
			}
			filePath = filePath+clazz.getSimpleName()+".log";
		}
		return filePath;
	}
	
	private static Logger create(Class<?> clazz, String filePath) {
		Logger logger = Logger.getLogger(clazz.getCanonicalName());
		try {
			boolean append = true;
			FileHandler fileHandler = new FileHandler(filePath, append);   
			LoggerFormatter loggerFormatter = new LoggerFormatter();
			fileHandler.setFormatter(loggerFormatter);
			logger.addHandler(fileHandler); 
			logger.setUseParentHandlers(false);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return logger;
	}
	
	private static Logger getLogger(Class<?> clazz, String filePath) {
		Logger logger = null;
		if(filePath != null) {
			logger = create(clazz, filePath);
		}
		return logger;
	}
	
	private static Logger getLogger(Class<?> clazz) {
		String filePath = getFilePath(clazz);
		Logger logger = getLogger(clazz, filePath);
		return logger;
	}
	
	// Note: close to insure that lock file is erased
	
	private static void close(Logger logger) {
		Handler[] handlers = logger.getHandlers();
		if(handlers != null) {
			for(Handler handler : handlers) {
				handler.close();
			}
		}
	}
	
	/**
	 * Write an "info"  String message into ErrorHandler.log file
	 */
	public static void info(Class<?> clazz, String text) {
		synchronized(ToLog.class) {
			if(clazz != null) {
				if(text != null) {
					Logger logger = getLogger(clazz);
					if(logger != null) {
						logger.info(text);
						close(logger);
					}
				}
			}
		}
	}
	
	/**
	 * Write a "debug" String message into ErrorHandler.log file
	 */
	public static void debug(Class<?> clazz, String text) {
		synchronized(ToLog.class) {
			if(clazz != null) {
				if(text != null) {
					Logger logger = getLogger(clazz);
					if(logger != null) {
						logger.log(Level.FINE, text);
						close(logger);
					}
				}
			}			
		}
	}
	
	/**
	 * Write a "warning" String message into ErrorHandler.log file
	 */
	public static void warning(Class<?> clazz, String text) {
		synchronized(ToLog.class) {
			if(clazz != null) {
				if(text != null) {
					Logger logger = getLogger(clazz);
					if(logger != null) {
						logger.log(Level.WARNING, text);
						close(logger);
					}
				}
			}
		}
	}
	
	/**
	 * Write a Throwable message into ErrorHandler.log file
	 */
	public static void warning(Class<?> clazz, Throwable t) {
		synchronized(ToLog.class) {
			if(clazz != null) {
				if(t != null) {
					Logger logger = getLogger(clazz);
					if(logger != null) {
						logger.log(Level.WARNING, t.getMessage(), t);
						close(logger);
					}
				}
			}
		}
	}

}
