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

public class DuccConstants {

	public static final String FAIR_SHARE = "FAIR_SHARE";
	public static final String FIXED_SHARE = "FIXED_SHARE";
	public static final String RESERVE = "RESERVE";
	public static final String JobDriver = "JobDriver";
	
	public static final int defaultShareSize = 15;
	public static final int[] memorySizes = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
	
	public static final String hintPreferencesDescriptionStyleShort = "Hint: use Preferences -> Description Style [Short] to hide everything left of last /";
	public static final String hintPreferencesRoleAdministrator = "Hint: use Preferences -> Role [Administrator] to enable";
	public static final String hintLogin = "Hint: use Login to enable";
}
