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

package org.apache.uima.ducc.ws.server;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Properties;

import org.apache.uima.ducc.cli.DuccUiConstants;
import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;

public class DuccFile {
	
	private static String ducc_ling = 
			Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());
	
	public static Properties getUserSpecifiedProperties(IDuccWorkJob job, String user) throws Throwable {
		String directory = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
		String name = DuccUiConstants.user_specified_properties;
		Properties properties = null;
		try {
			properties = DuccFile.getProperties(directory+name, user);
		}
		catch(Exception e) {
			// no worries
		}
		return properties;
	}
	
	public static Properties getFileSpecifiedProperties(IDuccWorkJob job, String user) throws Throwable {
		String directory = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
		String name = DuccUiConstants.file_specified_properties;
		Properties properties = null;
		try {
			properties = DuccFile.getProperties(directory+name, user);
		}
		catch(Exception e) {
			// no worries
		}
		return properties;
	}
	
	public static Properties getJobProperties(IDuccWorkJob job, String user) throws Throwable {
		String directory = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
		String name = DuccUiConstants.job_specification_properties;
		Properties properties = DuccFile.getProperties(directory+name, user);
		return properties;
	}
	
	public static Properties getManagedReservationProperties(IDuccWorkJob job, String user) throws Throwable {
		String directory = job.getUserLogsDir()+job.getDuccId().getFriendly()+File.separator;
		// <hack>
		try {
			String hack_name = "process.properties";
			Properties hack_properties = DuccFile.getProperties(directory+hack_name, user);
			if(!hack_properties.isEmpty()) {
				return hack_properties;
			}
		}
		catch(Exception e) {
		}
		// </hack>
		String name = DuccUiConstants.managed_reservation_properties;
		Properties properties = DuccFile.getProperties(directory+name);
		return properties;
	}
	
	/*
	public static Properties getProperties(String directory, String name) throws IOException {
		return getProperties(directory+name);
	}
	*/
	
	private static Properties getProperties(String path) throws IOException {
		FileInputStream fis = null;
		try {
			File file = new File(path);
			fis = new FileInputStream(file);
			Properties properties = new Properties();
			properties.load(fis);
			fis.close();
			return properties;
		}
		catch(IOException e) {
			if(fis != null) {
				fis.close();
			}
			throw e;
		}
	}
	
	public static Properties getProperties(String path, String user) throws Throwable {
		if(user == null) {
			return getProperties(path);
		}
		StringReader sir = null;
		try {
			AlienFile alienFile = new AlienFile(user, path, ducc_ling);
			String data = alienFile.getString();
			sir = new StringReader(data);
			Properties properties = new Properties();
			properties.load(sir);
			sir.close();
			return properties;
		}
		finally {
			try {
				if(sir != null) {
					sir.close();
				}
			}
			catch(Throwable t) {
			}
		}
	}
	
	private static InputStreamReader getInputStreamReader(String path) throws IOException {
		InputStreamReader isr = null;
		FileInputStream fis = new FileInputStream(path);
		DataInputStream dis = new DataInputStream(fis);
		isr = new InputStreamReader(dis);
		return isr;
	}
	
	
	public static InputStreamReader getInputStreamReader(String path, String user) throws Throwable {
		if(user == null) {
			return getInputStreamReader(path);
		}
		AlienFile alienFile = new AlienFile(user, path, ducc_ling);
		return alienFile.getInputStreamReader();
	}
}
