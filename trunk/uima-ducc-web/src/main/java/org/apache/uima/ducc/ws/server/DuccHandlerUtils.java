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

import java.text.DecimalFormat;

public class DuccHandlerUtils {

	public static String warn(String text) {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_red\""+">");
		sb.append(text);
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String down() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_red\""+">");
		sb.append("down");
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String up() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_green\""+">");
		sb.append("up");
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String disabled() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_black\""+">");
		sb.append("disabled");
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String up_provisional(String text) {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_black\""+">");
		sb.append("up"+text);
		sb.append("</span>");
		return sb.toString();
	}
	
	public static String unknown() {
		StringBuffer sb = new StringBuffer();
		sb.append("<span class=\"health_black\""+">");
		sb.append("unknown");
		sb.append("</span>");
		return sb.toString();
	}
	
	// *****
	
	private static DecimalFormat formatter = new DecimalFormat("###0.0");
	
	public static String getSwapSizeDisplay(double swapBytes) {
		String retVal = formatter.format(swapBytes/GB);;
		return retVal;
	}
	
	public static double GB = Math.pow(10,9);
	public static double MB = Math.pow(10,6);
	public static double KB = Math.pow(10,3);

	public static String getSwapSizeHover(double swapBytes) {
		String retVal = null;
		if(swapBytes == 0) {
			retVal = formatter.format(swapBytes/GB)+" "+"GB";
		}
		else if(swapBytes >= GB/10) {
			retVal = formatter.format(swapBytes/GB)+" "+"GB";
		}
		else if(swapBytes >= MB/10) {
			retVal = formatter.format(swapBytes/MB)+" "+"MB";
		}
		else if(swapBytes >= KB/10) {
			retVal = formatter.format(swapBytes/KB)+" "+"KB";
		}
		else {
			retVal = formatter.format(swapBytes)+" "+"Bytes";
		}
		return retVal;
	}
	
	
	
	
	
}
