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
package org.apache.uima.ducc.orchestrator.user;

import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;

public class UserLogging {

	private static String ducc_log = "ducc.log";
	private static String ducc_error = "ducc.error";

    public static void record(IDuccWorkJob job, String text) {
        UserLogging.write(job, ducc_log, text);
    }

    public static void error(IDuccWorkJob job, String text) {
        UserLogging.write(job, ducc_error, text);
    }

	static void write(IDuccWorkJob job, String fname, String text) {
		if(text != null) {
	        String user = job.getStandardInfo().getUser();
	        String umask = job.getStandardInfo().getUmask();
	        String file = job.getUserLogDir() + fname;
			String millis = ""+System.currentTimeMillis();
			String ts = TimeStamp.simpleFormat(millis);
			DuccAsUser.duckling(user, umask, file, ts+" "+text);
		}
	}
}
