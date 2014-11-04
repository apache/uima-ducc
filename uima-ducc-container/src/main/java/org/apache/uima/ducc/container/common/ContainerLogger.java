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
package org.apache.uima.ducc.container.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class ContainerLogger implements IContainerLogger {

	static public ContainerLogger getLogger(@SuppressWarnings("rawtypes") Class clazz, String component) {
		return new ContainerLogger(clazz, component);
	}
	
	private static enum Type { 
		DEBUG(true), INFO(true), WARN(true), ERROR(true), TRACE(false);
		private boolean active = true;
		private Type(boolean value) {
			active = value;
		}
		public boolean isActive() {
			return(active);
		}
	};
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
	
	public ContainerLogger(@SuppressWarnings("rawtypes") Class clazz, String component) {
		setClazz(clazz);
		setComponent(component);
	}
	
	private String clazz = null;
	private String component = null;
	
	private boolean displayComponent = false;
	
	private void setClazz(Class<?> value) {
		clazz = value.getSimpleName();
	}

	private void setComponent(String value) {
		component = value;
	}
	
	private String format(Object...args) {
		StringBuffer text = new StringBuffer();
		if(args != null) {
			for(Object arg : args) {
				if(arg != null) {
					if(arg instanceof String) {
						text.append(arg);
					}
					else {
						text.append(arg.toString());
					}
					text.append(" ");	
				}
			}
		}
		return text.toString().trim();
	}
	
	private void log(Type type, String location, IEntityId jobid, Object... args) {
		StringBuffer text = new StringBuffer();
		Date date = Calendar.getInstance().getTime();
		text.append(sdf.format(date));
		text.append(" ");
		text.append(type.name());
		text.append(" ");
		if(displayComponent) {
			text.append(component);
			text.append(":");
		}
		text.append(clazz);
		text.append(".");
		text.append(location);
		text.append(" ");	
		text.append(format(args));
		System.out.println(text);
	}
	
	@Override
	public void debug(String location, IEntityId eid, Object... args) {
		if(Type.DEBUG.isActive()) {
			log(Type.DEBUG, location, eid, args);
		}
	}
	
	@Override
	public void error(String location, IEntityId eid, Object... args) {
		if(Type.ERROR.isActive()) {
			log(Type.ERROR, location, eid, args);
		}
	}
	
	@Override
	public void info(String location, IEntityId eid, Object... args) {
		if(Type.INFO.isActive()) {
			log(Type.INFO, location, eid, args);
		}
	}
	
	@Override
	public void trace(String location, IEntityId eid, Object... args) {
		if(Type.TRACE.isActive()) {
			log(Type.TRACE, location, eid, args);
		}
	}
}
