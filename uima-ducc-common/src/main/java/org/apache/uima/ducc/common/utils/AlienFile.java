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
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.uima.ducc.common.utils.id.DuccId;

public class AlienFile {
	
	private static DuccId duccId = null;
	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(AlienFile.class.getName());
	
	private String ducc_ling;
	private String q_parameter = "-q";
	private String u_parameter = "-u";
	private String user;
	private String command_cat = "/bin/cat";
	private String file_name;
	private String encoding = "UTF-8";
	
	protected void set_ducc_ling(String value) {
		ducc_ling = value;
	}
	
	protected String get_ducc_ling() {
		return ducc_ling;
	}
	
	protected void set_u_parameter(String value) {
		u_parameter = value;
	}
	
	protected String get_u_parameter() {
		return u_parameter;
	}
	
	protected void set_q_parameter(String value) {
		q_parameter = value;
	}
	
	protected String get_q_parameter() {
		return q_parameter;
	}
	
	protected void set_user(String value) {
		user = value;
	}
	
	protected String get_user() {
		return user;
	}
	
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
	
	protected void set_encoding(String value) {
		encoding = value;
	}
	
	protected String get_encoding() {
		return encoding;
	}
	
	/*
	public AlienFile(String user, String file_name) {
		set_user(user);
		set_file_name(file_name);
	}
	*/
	
	public AlienFile(String user, String file_name, String ducc_ling) {
		set_user(user);
		set_file_name(file_name);
		set_ducc_ling(ducc_ling);
	}
	
	private void echo(String[] command) {
		String methodName = "echo";
		try {
			StringBuffer sb = new StringBuffer();
			for(String token : command) {
				sb.append(" ");
				sb.append(token);
			}
			String text = sb.toString().trim();
			duccLogger.debug(methodName, duccId, text);
		}
		catch(Throwable t) {
			t.printStackTrace();
		}
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
	
	private int getByteSize() throws Throwable {
		String methodName = "getByteSize";
		String[] command = getCommand();
		echo(command);
		ProcessBuilder pb = new ProcessBuilder( command );
		Process p = pb.start();
		p.waitFor();
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
        int rc = 0;
        while(rc >= 0) {
			rc = br.read();
	        size++;
		}
        String text = ""+size;
        duccLogger.debug(methodName, duccId, text);
        return size;
	}
	
	private void closer(Closeable c) {
		try {
			c.close();
		}
		catch(Throwable t) {
		}
	}
	
	private String reader(int size) throws Throwable {
		String data = null;
		BufferedReader br = null;
		InputStreamReader isr = null;
		try {
			String[] command = getCommand();
			echo(command);
			ProcessBuilder pb = new ProcessBuilder( command );
			Process p = pb.start();
			p.waitFor();
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
		catch(Throwable t) {
			t.printStackTrace();
			throw t;
		}
		finally {
			closer(br);
			closer(isr);
		}
	}
	
	public String getString() throws Throwable {
		int size = getByteSize();
		String data = reader(size);
		if(data != null) {
			data = data.trim();
		}
		return data;
	}
	
	public InputStreamReader getInputStreamReader() throws Throwable {
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
		catch(Throwable t) {
			throw t;
		}
		
		return isr;
	}

	public DataInputStream getDataInputStream() throws Throwable {
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
		catch(Throwable t) {
			throw t;
		}
		
		return dis;
	}
	
	public static void main(String[] args) throws Throwable {
		AlienFile alienFile;
		String arg_user = args[0];
		String arg_file = args[1];
		if(args.length < 3) {
			alienFile = new AlienFile(arg_user, arg_file, null);
		}
		else {
			String arg_ducc_ling = args[2];
			alienFile = new AlienFile(arg_user, arg_file, arg_ducc_ling);
		}
		String data = alienFile.getString();
		System.out.println(data);
	}
}
