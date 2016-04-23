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
package org.apache.uima.ducc;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.logger.ToLog;

public class ErrorHandlerProgrammability {

	/**
	 * This class interprets the <string> specified for
	 * --driver_exception_handler_arguments of the ducc_submit command.
	 */
	
	private Map<Key,Integer> map = new ConcurrentHashMap<Key,Integer>();
	
	/**
	 * Supported keywords for 
	 * ducc_submit --driver_exception_handler_arguments <string>
	 */
	public enum Key { 
		max_job_errors,
		max_timeout_retrys_per_workitem,
		;
	}
	
	/**
	 * Initialize with defaults
	 */
	{
		map.put(Key.max_job_errors, new Integer(15));
		map.put(Key.max_timeout_retrys_per_workitem, new Integer(0));
	}
	
	public ErrorHandlerProgrammability() {
		dumpArgs();
		dumpMap();
	}
	
	/**
	 * @param argString is a blank-delimited String of key=value pairs 
	 *        with no embedded blanks.  Example: keyA=valA keyB=valB...
	 */
	
	public ErrorHandlerProgrammability(String argString) {
		dumpArgs(argString);
		String[] args = toArgs(argString);
		override(args);
		dumpMap();
	}
	
	private String[] toArgs(String argString) {
		String[] list = null;
		if(argString != null) {
			list = argString.split("\\s+");
		}
		return list;
	}
	
	/**
	 * @param args is an array of key=value Strings
	 */
	
	public ErrorHandlerProgrammability(String[] args) {
		dumpArgs(args);
		override(args);
		dumpMap();
	}
	
	/**
	 * @param args is an array of key=value Strings
	 * 
	 * Replace the default values in the map with those specified
	 * by the passed in args
	 */
	private void override(String[] args) {
		if(args != null) {
			for(String arg : args) {
				String[] kvp = arg.split("=");
				if(kvp.length == 2) {
					Key key = parseKey(kvp[0]);
					Integer value = parseValue(kvp[1]);
					map.put(key,value);
					String text = "override: "+key.name()+"="+value;
					ToLog.info(ErrorHandler.class,text);
				}
				else {
					String text = "illegal argument: "+arg;
					ToLog.warning(ErrorHandler.class,text);
				}
			}
		}
	}
	
	/**
	 * @param name is one of the expected String
	 * @return Key is the corresponding enum
	 */
	
	private Key parseKey(String name) {
		Key key = null;
		if(name != null) {
			for(Key k : Key.values()) {
				if(name.equals(k.name())) {
					key = k;
					break;
				}
			}
			if(key == null) {
				String text = "illegal argument: "+name;
				ToLog.warning(ErrorHandler.class,text);
			}
		}
		else {
			String text = "missing argument: "+"<name>=";
			ToLog.warning(ErrorHandler.class,text);
		}
		return key;
	}
	
	/**
	 * 
	 * @param value is a String representation of an integer
	 * @return the value as an Integer
	 */
	private Integer parseValue(String value) {
		int iVal = 0;
		if(value != null) {
			try {
				iVal = Integer.parseInt(value);
			}
			catch(Exception e) {
				String text = "illegal argument: "+value;
				ToLog.warning(ErrorHandler.class,text);
			}
		}
		else {
			String text = "missing argument: "+"=<value>";
			ToLog.warning(ErrorHandler.class,text);
		}
		return iVal;
	}
	
	public Integer getInteger(Key key) {
		return map.get(key);
	}
	
	// The below methods are for debugging and are nominally silent
	
	private void dumpArgs() {
		String text = "args: "+"none";
		ToLog.debug(ErrorHandler.class,text);
	}
	
	public void dumpArgs(String args) {
		if(args == null) {
			String text = "argString: "+"null";
			ToLog.debug(ErrorHandler.class,text);
		}
		else {
			String text = "argString: "+args;
			ToLog.debug(ErrorHandler.class,text);
		}
	}
	
	public void dumpArgs(String[] args) {
		if(args == null) {
			String text = "args: "+"null";
			ToLog.debug(ErrorHandler.class,text);
		}
		else {
			StringBuffer sb = new StringBuffer();
			for(String arg : args) {
				sb.append(arg);
				sb.append(" ");
			}
			String text = "args: "+sb.toString().trim();
			ToLog.debug(ErrorHandler.class,text);
		}
	}
	
	private void dumpMap() {
		for(Entry<Key, Integer> entry : map.entrySet()) {
			String key = entry.getKey().name();
			Integer value = entry.getValue();
			String text = key+"="+value;
			ToLog.debug(ErrorHandler.class,text);
		}
	}
}
