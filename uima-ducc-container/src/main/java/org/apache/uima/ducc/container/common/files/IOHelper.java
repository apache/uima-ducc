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
package org.apache.uima.ducc.container.common.files;

import java.io.File;

public class IOHelper {
	
	public static String marryDir2File(String dirname, String filename) {
		StringBuffer sb = new StringBuffer();
		String dname = "";
		String fname = "";
		if(dirname != null) {
			dname = dirname;
		}
		if(filename != null) {
			fname = filename;
		}
		if(dname.endsWith(File.separator)) {
			sb.append(dirname);
		}
		else {
			sb.append(dirname);
			sb.append(File.separator);
		}
		if(fname.startsWith(File.separator)) {
			sb.append(fname.substring(1));
		}
		else {
			sb.append(fname);
		}
		return sb.toString();	
	}
	
	public static boolean mkdirs(String directory) {
		boolean created = false;
		File file = new File(directory);
		if(!file.exists()) {
			file.mkdirs();
			created = true;
		}
		return created;
	}
}
