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

package org.apache.uima.ducc.database.lifetime;

/*
 * Class comprising common methods and data for this package.
 */
public class DbDaemonLifetimeCommon {
	
	public static final Integer RC_Failure = new Integer(-1);
	public static final Integer RC_Success = new Integer(0);
	public static final Integer RC_Help = new Integer(1);
	
	public static String normalize(String value) {
		String retVal = value;
		if(value != null) {
			retVal = value.toLowerCase();
		}
		return retVal;
	}
	
	public static String normalize_kw(String kw) {
		String retVal = normalize(kw);
		return retVal;
	}
	
	public static String normalize_daemon(String daemon) {
		String retVal = normalize(daemon);
		return retVal;
	}
	
	public static String normalize_host(String host) {
		String retVal = normalize(host);
		if(host != null) {
			retVal = host.split("\\.")[0];
		}
		return retVal;
	}
}
