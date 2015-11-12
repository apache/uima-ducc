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
package org.apache.uima.ducc.common.utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class AlienFile extends AlienAbstract {
	
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(AlienFile.class.getName());
	
	private static String ducc_ling = 
			Utils.resolvePlaceholderIfExists(
					System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());
	
	private String command_cat = "/bin/cat";
	private String file_name;
	
	protected void set_command_cat(String value) {
		command_cat = value;
	}
	
	protected String get_command_cat() {
		return command_cat;
	}
	
	protected void set_file_name(String value) {
		file_name = value;
	}
	
	protected String get_file_name() {
		return file_name;
	}
	
	public AlienFile(String user, String file_name) {
		set_user(user);
		set_file_name(file_name);
	}
	
	private String[] getCommand() {
		String[] command_ducc_ling_yes = { ducc_ling, q_parameter, u_parameter, user, command_cat, file_name};
		String[] command_ducc_ling_no  = { command_cat, file_name};
		String[] command = command_ducc_ling_yes;
		if(ducc_ling == null) {
			command = command_ducc_ling_no;
		}
		return command;
	}
	
	private int getByteSize() throws Exception {
		String methodName = "getByteSize";
		String[] command = getCommand();
		echo(command);
		ProcessBuilder pb = new ProcessBuilder( command );
		pb = pb.redirectErrorStream(true);
		Process p = pb.start();
		InputStream pOut = p.getInputStream();
		InputStreamReader isr;
		if(FileHelper.isGzFileType(file_name)) {
			GZIPInputStream gis = new GZIPInputStream(pOut);
			isr = new InputStreamReader(gis, encoding);
		}
		else {
			isr = new InputStreamReader(pOut);
		}
		BufferedReader br = new BufferedReader(isr);
		int size = 0;
        int readChar = 0;
        while(readChar >= 0) {
			readChar = br.read();
	        size++;
		}
        int rc = p.waitFor();
        String text = "rc="+rc+" "+"size="+size;
        duccLogger.debug(methodName, duccId, text);
        return size;
	}

	private String reader(int size) throws Exception {
		String data = null;
		BufferedReader br = null;
		InputStreamReader isr = null;
		try {
			String[] command = getCommand();
			echo(command);
			ProcessBuilder pb = new ProcessBuilder( command );
			Process p = pb.start();
			//p.waitFor();
			InputStream pOut = p.getInputStream();
			if(FileHelper.isGzFileType(file_name)) {
				GZIPInputStream gis = new GZIPInputStream(pOut);
				isr = new InputStreamReader(gis, encoding);
			}
			else {
				isr = new InputStreamReader(pOut);
			}
			char[] cbuf = new char[size];
	        br = new BufferedReader(isr);
	        br.read(cbuf);
	        data = new String(cbuf);
			return data;
		}
		catch(Exception e) {
			e.printStackTrace();
			throw e;
		}
		finally {
			closer(br);
			closer(isr);
		}
	}
	
	public String getString() throws Exception {
		int size = getByteSize();
		String data = reader(size);
		if(data != null) {
			data = data.trim();
		}
		return data;
	}
	
	public InputStreamReader getInputStreamReader() throws Exception {
		InputStreamReader isr = null;
		try {
			String[] command = getCommand();
			echo(command);
			ProcessBuilder pb = new ProcessBuilder( command );
			Process p = pb.start();
			//p.waitFor();
			InputStream pOut = p.getInputStream();
			if(FileHelper.isGzFileType(file_name)) {
				GZIPInputStream gis = new GZIPInputStream(pOut);
				isr = new InputStreamReader(gis, encoding);
			}
			else {
				isr = new InputStreamReader(pOut);
			}
		} 
		catch(Exception e) {
			throw e;
		}
		return isr;
	}

	public DataInputStream getDataInputStream() throws Exception {
		DataInputStream dis = null;
		try {
			String[] command = getCommand();
			echo(command);
			ProcessBuilder pb = new ProcessBuilder( command );
			Process p = pb.start();
			//p.waitFor();
			InputStream pOut = p.getInputStream();
			dis= new DataInputStream(pOut);
		} 
		catch(Exception e) {
			throw e;
		}
		return dis;
	}
	
	public static void main(String[] args) throws Exception {
		AlienFile alienFile;
		String arg_user = args[0];
		String arg_file = args[1];
		alienFile = new AlienFile(arg_user, arg_file);
		String data = alienFile.getString();
		System.out.println(data);
	}
}
