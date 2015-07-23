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
package org.apache.uima.ducc.ws.utils.alien;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.uima.ducc.ws.server.AlienTextFile;

public class FileInfo {
	
	public String reldir = null;
	public String permissions = null;
	public String user = null;
	public String group = null;
	public long length = 0;
	public String date = null;
	public String time = null;
	public String name = null;
	
	public FileInfo(String reldir, String permissions, String user, String group, long length, String date, String time, String name) {
		setRelDir(reldir);
		setPermissions(permissions);
		setUser(user);
		setGroup(group);
		setLength(length);
		setDate(date);
		setTime(time);
		setName(name);
	}
	
	public void setRelDir(String value) {
		reldir = value;
	}
	
	public String getRelDir() {
		return reldir;
	}
	
	public void setPermissions(String value) {
		permissions = value;
	}
	
	public String getPermissions() {
		return permissions;
	}
	
	public boolean isDirectory() {
		boolean retVal = false;
		if(permissions != null) {
			if(permissions.startsWith("d")) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	public void setUser(String value) {
		user = value;
	}
	
	public String getUser() {
		return user;
	}
	
	public void setGroup(String value) {
		group = value;
	}
	
	private DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
	private SimpleDateFormat dow = new SimpleDateFormat("E"); // the day of the week abbreviated
	
	public long getTOD() {
		long retVal = 0;
		try {
			String dt = getDate()+" "+getTime();
			Date date = format.parse(dt);
			retVal = date.getTime();
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	public String getDOW() {
		String retVal = "";
		try {
			String dt = getDate()+" "+getTime();
			Date date = format.parse(dt);
			retVal = dow.format(date);
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	public long getLastModified() {
		return getTOD();
	}
	
	public String getGroup() {
		return group;
	}
	
	public void setLength(long value) {
		length = value;
	}
	
	public long getLength() {
		return length;
	}
	
	public int getPageCount() {
		int retVal = 0;
		int pageSize = AlienTextFile.get_page_bytes();
		long fileBytes = length;
		retVal = (int) Math.ceil(fileBytes / (1.0 * pageSize));
		return retVal;
	}
	
	public void setDate(String value) {
		date = value;
	}
	
	public String getDate() {
		return date;
	}
	
	public void setTime(String value) {
		time = value;
	}
	
	public String getTime() {
		return time;
	}
	
	public void setName(String value) {
		name = value;
	}
	
	public String getName() {
		return name;
	}
	
	public String getShortName() {
		String retVal = name;
		if(name != null) {
			retVal = name.substring(name.lastIndexOf(File.separator)+1);
		}
		return retVal;
	}
}
