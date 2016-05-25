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
package org.apache.uima.ducc.cli.aio;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.cli.IDuccCallback;

public class MessageHandler implements IMessageHandler {
	
	private ConcurrentHashMap<Level,Toggle> map = new ConcurrentHashMap<Level,Toggle>();
	
	private Toggle timestamping = Toggle.Off;
	private Toggle typeIdentifying = Toggle.On;
	
	private IDuccCallback consoleCb = null;
	
	public MessageHandler() {
		initialize();
	}
	
	public MessageHandler(IDuccCallback consoleCb) {
		this.consoleCb = consoleCb;
		initialize();
	}
	
	public MessageHandler(Toggle timestamping) {
		setTimestamping(timestamping);
		initialize();
	}
	
	private void initialize() {
		map.put(Level.UserInfo, Toggle.On);
		map.put(Level.UserError, Toggle.On);
		map.put(Level.UserWarn, Toggle.On);
		map.put(Level.UserDebug, Toggle.Off);
		map.put(Level.UserTrace, Toggle.Off);
		map.put(Level.FrameworkInfo, Toggle.Off);
		map.put(Level.FrameworkError, Toggle.Off);
		map.put(Level.FrameworkWarn, Toggle.Off);
		map.put(Level.FrameworkDebug, Toggle.Off);
		map.put(Level.FrameworkTrace, Toggle.Off);
	}

	public void setLevel(Level level, Toggle toggle) {
		if(level != null) {
			if(toggle != null) {
				map.put(level, toggle);
			}
		}	
	}

	public Toggle getLevel(Level level) {
		Toggle retVal = Toggle.Off;
		if(level != null) {
			if(map.containsKey(level)) {
				retVal = map.get(level);
			}
		}
		return retVal;
	}

	public void setTimestamping(Toggle toggle) {
		synchronized(timestamping) {
			if(toggle != null) {
				timestamping = toggle;
			}
		}
	}

	public Toggle getTimestamping() {
		synchronized(timestamping) {
			return timestamping;
		}
	}

	public void setTypeIdentifying(Toggle toggle) {
		synchronized(typeIdentifying) {
			if(toggle != null) {
				typeIdentifying = toggle;
			}
		}
	}

	public Toggle getTypeIdentifying() {
		synchronized(typeIdentifying) {
			return typeIdentifying;
		}
	}
	
	private void sysout(Level level, String message) {
		String text = message;
		switch(getTypeIdentifying()) {
		case On:
			if(level != null) {
				text = level.getLabel()+" "+message;
			}
		}
		switch(getTimestamping()) {
		case On:
			Date date = new Date();
			text = date+" "+text;
		}
		if(consoleCb != null) {
			consoleCb.status(text);
		}
		else {
			System.out.println(text);
			System.out.flush();
		}
	}
	
	private void syserr(Level level, String message) {
		String text = message;
		switch(getTypeIdentifying()) {
		case On:
			if(level != null) {
				text = level.getLabel()+" "+message;
			}
		}
		switch(getTimestamping()) {
		case On:
			Date date = new Date();
			text = date+" "+text;
		}
		if(consoleCb != null) {
			consoleCb.status(text);
		}
		else {
			System.err.println(text);
			System.err.flush();
		}
	}
	
	public void info(String message) {
		Toggle toggle = map.get(Level.UserInfo);
		switch(toggle) {
		case On:
			sysout(Level.UserInfo,message);
		}
	}
	
	public void error(String message) {
		Toggle toggle = map.get(Level.UserError);
		switch(toggle) {
		case On:
			syserr(Level.UserError, message);
		}
	}

	public void error(Exception e) {
		Toggle toggle = map.get(Level.UserError);
		switch(toggle) {
		case On:
			e.printStackTrace(System.err);
			System.err.flush();
		}
	}

	public void error(Throwable t) {
		Toggle toggle = map.get(Level.UserError);
		switch(toggle) {
		case On:
			t.printStackTrace(System.err);
			System.err.flush();
		}
	}

	public void warn(String message) {
		Toggle toggle = map.get(Level.UserWarn);
		switch(toggle) {
		case On:
			sysout(Level.UserWarn, message);
		}
	}
	
	public void debug(String message) {
		Toggle toggle = map.get(Level.UserDebug);
		switch(toggle) {
		case On:
			sysout(Level.UserDebug, message);
		}
	}

	public void trace(String message) {
		Toggle toggle = map.get(Level.UserTrace);
		switch(toggle) {
		case On:
			sysout(Level.UserTrace, message);
		}
	}

	public void info(String cid, String mid, String message) {
		info(cid+"."+mid+" "+message);
	}
	
	public void error(String cid, String mid, String message) {
		error(cid+"."+mid+" "+message);
	}
	
	public void warn(String cid, String mid, String message) {
		warn(cid+"."+mid+" "+message);
	}
	
	public void debug(String cid, String mid, String message) {
		debug(cid+"."+mid+" "+message);
	}
	
	public void trace(String cid, String mid, String message) {
		trace(cid+"."+mid+" "+message);
	}
	
	public void frameworkInfo(String message) {
		Toggle toggle = map.get(Level.FrameworkInfo);
		switch(toggle) {
		case On:
			sysout(Level.FrameworkInfo, message);
		}
	}

	public void frameworkError(String message) {
		Toggle toggle = map.get(Level.FrameworkError);
		switch(toggle) {
		case On:
			syserr(Level.FrameworkError, message);
		}
	}

	public void frameworkError(Exception e) {
		Toggle toggle = map.get(Level.FrameworkError);
		switch(toggle) {
		case On:
			e.printStackTrace(System.err);
			System.err.flush();
		}
	}

	public void frameworkError(Throwable t) {
		Toggle toggle = map.get(Level.FrameworkError);
		switch(toggle) {
		case On:
			t.printStackTrace(System.err);
			System.err.flush();
		}
	}

	public void frameworkWarn(String message) {
		Toggle toggle = map.get(Level.FrameworkWarn);
		switch(toggle) {
		case On:
			sysout(Level.FrameworkWarn, message);
		}
	}
	
	public void frameworkDebug(String message) {
		Toggle toggle = map.get(Level.FrameworkDebug);
		switch(toggle) {
		case On:
			sysout(Level.FrameworkDebug, message);
		}
	}

	public void frameworkTrace(String message) {
		Toggle toggle = map.get(Level.FrameworkTrace);
		switch(toggle) {
		case On:
			sysout(Level.FrameworkTrace, message);
		}
	}
	
	public void frameworkInfo(String cid, String mid, String message) {
		frameworkInfo(cid+"."+mid+" "+message);
	}
	
	public void frameworkError(String cid, String mid, String message) {
		frameworkError(cid+"."+mid+" "+message);
	}
	
	public void frameworkWarn(String cid, String mid, String message) {
		frameworkWarn(cid+"."+mid+" "+message);
	}
	
	public void frameworkDebug(String cid, String mid, String message) {
		frameworkDebug(cid+"."+mid+" "+message);
	}
	
	public void frameworkTrace(String cid, String mid, String message) {
		frameworkTrace(cid+"."+mid+" "+message);
	}

}
