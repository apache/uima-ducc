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
package org.apache.uima.ducc.rm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.Utils;

public class NodeStatus {

	private static String dir_home = Utils.findDuccHome();
	private static String dir_resources = "resources";
	private static String ducc_properties_filename = dir_home+File.separator+dir_resources+File.separator+"nodes.offline";
	
	private static NodeStatus instance = new NodeStatus();
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
	
	public static NodeStatus getInstance() {
		return instance;
	}
	
	private Properties properties = new Properties();
	
	public NodeStatus() {
		load();
	}
	
	private String normalize(String nodeName) {
		String retVal = nodeName;
		if(nodeName != null) {
			retVal = nodeName.trim();
		}
		return retVal;
	}
	
	private void load() {
		try {
			File file = new File(ducc_properties_filename);
			FileInputStream fis;
			fis = new FileInputStream(file);
			properties.load(fis);
			fis.close();
		} 
		catch(Throwable t) {
			handle(t);
		} 
	}
	
	private void store() {
		try {
			File file = new File(ducc_properties_filename);
			FileOutputStream fos;
			fos = new FileOutputStream(file);
			properties.store(fos, "");
			fos.close();
		} 
		catch(Throwable t) {
			handle(t);
		} 
	}
	
	public boolean isOffline(String nodeName) {
		boolean retVal = false;
		String name = normalize(nodeName);
		if(name != null) {
			load();
			retVal = properties.containsKey(name);
		}
		return retVal;
	}
	
	public boolean varyOffline(String nodeName) {
		boolean retVal = false;
		String name = normalize(nodeName);
		if(nodeName != null) {
			load();
	        Date date = new Date(System.currentTimeMillis());
			properties.put(name,""+sdf.format(date));
			store();
		}
		return retVal;
	}
	
	
	public boolean varyOnline(String nodeName) {
		boolean retVal = false;
		String name = normalize(nodeName);
		if(nodeName != null) {
			load();
			properties.remove(name);
			store();
		}
		return retVal;
	}
	
	private void handle(Throwable t) {
		t.printStackTrace();
	}
	
	//
	
	private static void _query(String nodeName) {
		NodeStatus nodeStatus = NodeStatus.getInstance();
		String state = "offline";
		if(!nodeStatus.isOffline(nodeName)) {
			state = "!offline";
		}
		System.out.println(nodeName+"="+state);
	}
	
	private static void _offline(String nodeName) {
		NodeStatus nodeStatus = NodeStatus.getInstance();
		nodeStatus.varyOffline(nodeName);
	}
	
	private static void _online(String nodeName) {
		NodeStatus nodeStatus = NodeStatus.getInstance();
		nodeStatus.varyOnline(nodeName);
	}
	
	public static void main(String[] args) {
		_offline("node9999");
		_online("node8888");
		_offline("node7777");
		_online("node7777");
		_query("node7777");
		_query("node8888");
		_query("node9999");
		_query("node0000");
	}
}
