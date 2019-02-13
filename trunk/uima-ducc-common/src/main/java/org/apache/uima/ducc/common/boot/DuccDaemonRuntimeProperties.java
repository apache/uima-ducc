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
package org.apache.uima.ducc.common.boot;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.InetHelper;
import org.apache.uima.ducc.common.utils.TimeStamp;


public class DuccDaemonRuntimeProperties {

	public static enum DaemonName {
		Broker,
		Database,
		Orchestrator,
		ResourceManager,
		ProcessManager,
		ServiceManager,
		Webserver,
	}
	
	public static DaemonName[] daemonNames = { 	
									DaemonName.Broker, 
									DaemonName.Database, 
									DaemonName.Orchestrator, 
									DaemonName.ProcessManager, 
									DaemonName.ResourceManager, 
									DaemonName.ServiceManager, 
									DaemonName.Webserver, 
									};
	
	public static String keyDaemonName = "daemonName";
	public static String keyBootTime = "bootTime";
	public static String keyBootType = "bootType";
	public static String keyNodeName = "nodeName";
	public static String keyNodeIpAddress = "nodeIpAddress";
	public static String keyPid = "pid";
	public static String keyJmxUrl = "jmxUrl";

	private static DuccDaemonRuntimeProperties instance = new DuccDaemonRuntimeProperties();
	
	public static DuccDaemonRuntimeProperties getInstance() {
		return instance;
	}
	
	private String ducc_daemons_dir = IDuccEnv.DUCC_DAEMONS_DIR;
	private String ducc_agents_dir = IDuccEnv.DUCC_AGENTS_DIR;
	private String suffix = "-boot.properties";
	
	private DuccDaemonRuntimeProperties() {
		init();
	}
	
	private void init() {
		IOHelper.mkdirs(ducc_daemons_dir);
		IOHelper.mkdirs(ducc_agents_dir);
	}
	
	private String getDaemonsHostDir() {
		return IDuccEnv.DUCC_DAEMONS_DIR+InetHelper.getHostName()+File.separator;
	}
	
	public Properties get(DaemonName daemonName) {
		Properties properties = new Properties();
		String fileName = getDaemonsHostDir()+daemonName+suffix;
		try {
			File file = new File(fileName);
			FileInputStream fis;
			fis = new FileInputStream(file);
			properties.load(fis);
			fis.close();
			
		} 
		catch (FileNotFoundException e) {
			System.out.println("File not found: "+fileName);
		} 
		catch (IOException e) {
			System.out.println("Error reading file: "+fileName);
		}
		return properties;
	}
	
	public void put(DaemonName daemonName, Properties properties) {
		
		String fileDir = getDaemonsHostDir();
		IOHelper.mkdirs(fileDir);
		String fileName = fileDir+daemonName+suffix;
		try {
			File file = new File(fileName);
			FileOutputStream fos;
			fos = new FileOutputStream(file);
			properties.store(fos,"");
			fos.close();
		} 
		catch (IOException e) {
			System.out.println("Error writing file: "+fileName);
		}
		return;
	}
	
	public void boot(DaemonName daemonName, String jmxUrl) {
		Properties bootProperties = new Properties();
		String daemonNameText = daemonName.toString();
		String bootTime = TimeStamp.simpleFormat(""+System.currentTimeMillis());
		String nodeIpAddress = "?";
		String nodeName = "?";
		String pid = "?";
		try {
			nodeIpAddress = InetAddress.getLocalHost().getHostAddress();
			nodeName = InetAddress.getLocalHost().getCanonicalHostName();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		try {
			pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		bootProperties.put(DuccDaemonRuntimeProperties.keyDaemonName, daemonNameText);
		bootProperties.put(DuccDaemonRuntimeProperties.keyBootTime, bootTime);
		bootProperties.put(DuccDaemonRuntimeProperties.keyJmxUrl, jmxUrl);
		bootProperties.put(DuccDaemonRuntimeProperties.keyNodeIpAddress, nodeIpAddress);
		bootProperties.put(DuccDaemonRuntimeProperties.keyNodeName, nodeName);
		bootProperties.put(DuccDaemonRuntimeProperties.keyPid, pid);
		getInstance().put(daemonName, bootProperties);
	}
	
	public Properties getAgent(String agentName) {
		Properties properties = new Properties();
		String fileName = IDuccEnv.DUCC_AGENTS_DIR+agentName+suffix;
		try {
			File file = new File(fileName);
			FileInputStream fis;
			fis = new FileInputStream(file);
			properties.load(fis);
			fis.close();
		} 
		catch (FileNotFoundException e) {
			System.out.println("File not found: "+fileName);
		} 
		catch (IOException e) {
			System.out.println("Error reading file: "+fileName);
		}
		return properties;
	}
	
	public void putAgent(String agentName, Properties properties) {
		String fileName = IDuccEnv.DUCC_AGENTS_DIR+agentName+suffix;
		try {
			File file = new File(fileName);
			FileOutputStream fos;
			fos = new FileOutputStream(file);
			properties.store(fos,"");
			fos.close();
		} 
		catch (IOException e) {
			System.out.println("Error writing file: "+fileName);
		}
		return;
	}
	
	public void bootAgent(String name, String ip, String jmxUrl) {
		Properties bootProperties = new Properties();
		String bootTime = TimeStamp.simpleFormat(""+System.currentTimeMillis());
		String nodeIpAddress = "?";
		String nodeName = "?";
		String pid = "?";
		try {
			nodeIpAddress = InetAddress.getLocalHost().getHostAddress();
			nodeName = InetAddress.getLocalHost().getCanonicalHostName();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		try {
			pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		bootProperties.put(DuccDaemonRuntimeProperties.keyDaemonName, name);
		bootProperties.put(DuccDaemonRuntimeProperties.keyBootTime, bootTime);
		bootProperties.put(DuccDaemonRuntimeProperties.keyJmxUrl, jmxUrl);
		bootProperties.put(DuccDaemonRuntimeProperties.keyNodeIpAddress, nodeIpAddress);
		bootProperties.put(DuccDaemonRuntimeProperties.keyNodeName, nodeName);
		bootProperties.put(DuccDaemonRuntimeProperties.keyPid, pid);
		getInstance().putAgent(name, bootProperties);
	}
}
