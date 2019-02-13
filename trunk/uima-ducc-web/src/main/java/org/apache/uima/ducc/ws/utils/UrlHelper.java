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
package org.apache.uima.ducc.ws.utils;

public class UrlHelper {

	public static String encode(String input) {
		String wip= new String(input);
		//
		wip = wip.replace("%", "%25");
		//
		wip = wip.replace("!", "%21");
		wip = wip.replace("#", "%23");
		wip = wip.replace("$", "%24");
		wip = wip.replace("&", "%26");
		wip = wip.replace("'", "%27");
		wip = wip.replace("(", "%28");
		wip = wip.replace(")", "%29");
		wip = wip.replace("*", "%2A");
		wip = wip.replace("+", "%2B");
		wip = wip.replace(",", "%2C");
		wip = wip.replace("/", "%2F");
		wip = wip.replace(":", "%3A");
		wip = wip.replace(";", "%3B");
		wip = wip.replace("=", "%3D");
		wip = wip.replace("?", "%3F");
		wip = wip.replace("@", "%40");
		wip = wip.replace("[", "%5B");
		wip = wip.replace("]", "%5D");
		String encoded = wip;
		return encoded;
	}
}
