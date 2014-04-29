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

import java.math.RoundingMode;
import java.text.DecimalFormat;

public class FormatHelper {
	
	private static DecimalFormat df = new DecimalFormat("#.0");
	
	static {
		df.setRoundingMode(RoundingMode.DOWN);
	}
	
	public static String duration(final long millis) {
		long seconds = millis / 1000;
		long dd =   seconds / 86400;
		long hh =  (seconds % 86400) / 3600;
		long mm = ((seconds % 86400) % 3600) / 60;
		long ss = ((seconds % 86400) % 3600) % 60;
		String text = String.format("%d:%02d:%02d:%02d", dd, hh, mm, ss);
		if(dd == 0) {
			text = String.format("%02d:%02d:%02d", hh, mm, ss);
			if(hh == 0) {
				text = String.format("%02d:%02d", mm, ss);
				if(mm == 0) {
					text = String.format("%02d", ss);
				}
			}
		}
		double subseconds = (millis%1000.0)/1000;
		String frac = df.format(subseconds);
		text = text+frac;
		return text;
	}
}
