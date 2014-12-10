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
package org.apache.uima.ducc.user.jp;

public class ArgsParser {
	/**
	 * scan args for a particular arg, return the following token or the empty
	 * string if not found
	 * 
	 * @param id
	 *            the arg to search for
	 * @param args
	 *            the array of strings
	 * @return the following token, or a 0 length string if not found
	 */
	public static String getArg(String id, String[] args) {
		for (int i = 0; i < args.length; i++) {
			if (id.equals(args[i]))
				return (i + 1 < args.length) ? args[i + 1] : "";
		}
		return "";
	}

	/**
	 * scan args for a particular arg, return the following token(s) or the
	 * empty string if not found
	 * 
	 * @param id
	 *            the arg to search for
	 * @param args
	 *            the array of strings
	 * @return the following token, or a 0 length string array if not found
	 */
	public static String[] getMultipleArg(String id, String[] args) {
		String[] retr = {};
		for (int i = 0; i < args.length; i++) {
			if (id.equals(args[i])) {
				String[] temp = new String[retr.length + 1];
				for (int s = 0; s < retr.length; s++) {
					temp[s] = retr[s];
				}
				retr = temp;
				retr[retr.length - 1] = (i + 1 < args.length) ? args[i + 1]
						: null;
			}
		}
		return retr;
	}

	/**
	 * scan args for a particular arg, return the following token(s) or the
	 * empty string if not found
	 * 
	 * @param id
	 *            the arg to search for
	 * @param args
	 *            the array of strings
	 * @return the following token, or a 0 length string array if not found
	 */
	public static String[] getMultipleArg2(String id, String[] args) {
		String[] retr = {};
		for (int i = 0; i < args.length; i++) {
			if (id.equals(args[i])) {
				int j = 0;
				while ((i + 1 + j < args.length)
						&& !args[i + 1 + j].startsWith("-")) {
					String[] temp = new String[retr.length + 1];
					for (int s = 0; s < retr.length; s++) {
						temp[s] = retr[s];
					}
					retr = temp;
					retr[retr.length - 1] = args[i + 1 + j++];
				}
				return retr;
			}
		}
		return retr;
	}

}
