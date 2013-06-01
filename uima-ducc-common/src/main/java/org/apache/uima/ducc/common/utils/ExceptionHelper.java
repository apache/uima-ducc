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

public class ExceptionHelper {

	public static String summarize(Exception e) {
		StringBuffer sb = new StringBuffer();
		if(e != null) {
			sb.append(e.getClass().getCanonicalName());
			StackTraceElement[] steArray = e.getStackTrace();
			if (steArray != null) {
				if(steArray.length > 0) {
					StackTraceElement ste = steArray[0];
					sb.append(" at ");
					sb.append(ste.getClassName());
					sb.append(".");
					sb.append(ste.getMethodName());
					sb.append("(");
					sb.append(ste.getFileName());
					sb.append(":");
					sb.append(ste.getLineNumber());
					sb.append(")");
				}
				
			}
		}
		return sb.toString();
	}
}
