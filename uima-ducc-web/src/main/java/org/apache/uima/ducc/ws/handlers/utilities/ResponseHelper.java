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
package org.apache.uima.ducc.ws.handlers.utilities;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.ws.log.WsLog;
import org.apache.uima.ducc.ws.server.DuccCookies.DateStyle;

public class ResponseHelper {
	
  //NOTE - this variable used to hold the class name before WsLog was simplified
	private static DuccLogger cName = DuccLogger.getLogger(ResponseHelper.class);
	
	public static String trStart(int counter) {
		if((counter % 2) > 0) {
			return "<tr class=\"ducc-row-odd\">";
		}
		else {
			return "<tr class=\"ducc-row-even\">";
		}
	}

	public static String trEnd(int counter) {
		return "</tr>";
	}

	public static String getTimeStamp(DateStyle dateStyle, String date) {
		String mName = "getTimeStamp";
		StringBuffer sb = new StringBuffer();
		if(date != null) {
			sb.append(date);
			if(date.trim().length() > 0) {
				try {
					switch(dateStyle) {
					case Long:
						break;
					case Medium:
						String day = sb.substring(sb.length()-4);
						sb.delete(0, 5);
						sb.delete(sb.lastIndexOf(":"), sb.length());
						sb.append(day);
						break;
					case Short:
						sb.delete(0, 5);
						sb.delete(sb.lastIndexOf(":"), sb.length());
						break;
					}
				}
				catch(Exception e) {
					WsLog.error(cName, mName, e);
				}
			}
		}
		return sb.toString();
	}
}
