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
package org.apache.uima.ducc.user.common;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionHelper {

	public static Exception wrapStringifiedException(Exception e) {
		Exception retVal = new Exception(toString(e));
		return retVal;
	}
	
	private static String toString(Exception e) {
		String retVal = null;
		try {
			if(e == null) {
				retVal = "exception is null?";
			}
			else {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				retVal = sw.toString();
			}
		}
		catch(Exception x) {
			retVal = "exception obtaining stack trace?";
			toConsole(e);
		}
		return retVal;
	}
	
	private static void toConsole(Exception e) {
		try {
			e.printStackTrace();
		}
		catch(Exception x) {
			// oh well
		}
	}
}
