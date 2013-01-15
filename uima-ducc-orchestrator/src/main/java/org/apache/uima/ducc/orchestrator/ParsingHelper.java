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
package org.apache.uima.ducc.orchestrator;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/* 
 *  inspired by => http://stackoverflow.com/questions/3366281/tokenizing-a-string-but-ignoring-delimiters-within-quotes
 */

public class ParsingHelper {
	
	private static String regex = "\"([^\"]*)\"|(\\S+)";

	public static List<String> parse(String text) {
		List<String> list = new ArrayList<String>();
		if(text != null) {
			Matcher m = Pattern.compile(regex).matcher(text);
		    while (m.find()) {
		        if (m.group(1) != null) {
		            //System.out.println("Quoted [" + m.group(1) + "]");
		        	list.add(m.group(1));
		        }
		        else if (m.group(2) != null) {
		            //System.out.println("Plain [" + m.group(2) + "]");
		        	list.add(m.group(2));
		        } 
		    }
		}
	    return list;
	}
    
}
