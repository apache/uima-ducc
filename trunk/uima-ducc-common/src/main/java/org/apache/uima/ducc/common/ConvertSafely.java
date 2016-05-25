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
package org.apache.uima.ducc.common;

public class ConvertSafely {

	/**
	 * Convert String to long, else zero
	 */
	public static long String2Long(String value) {
		long retVal = 0;
		try{
			retVal = Long.parseLong(value);
		}
		catch(Exception e) {
			// oh well
		}
		return retVal;
	}
	
	/**
	 * Convert String to double, else zero
	 */
	public static double String2Double(String value) {
		double retVal = 0;
		try{
			retVal = Double.parseDouble(value);
		}
		catch(Exception e) {
			// oh well
		}
		return retVal;
	}
	
}
