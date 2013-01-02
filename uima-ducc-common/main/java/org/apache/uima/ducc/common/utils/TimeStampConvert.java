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

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeStampConvert {

	public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSZ");
	
	public static Date simpleFormat(String formattedDate) {
		Date date = null;
		if(formattedDate != null) {
			try {
				String fixedDate = "";
				String[] dateParts = formattedDate.split(":");
				for(int i=0; i < dateParts.length; i++) {
					fixedDate += dateParts[i];
					if(i+2 < dateParts.length) {
						fixedDate+=":";
					}
				}
				date = simpleDateFormat.parse(fixedDate);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return date;
	}

}
