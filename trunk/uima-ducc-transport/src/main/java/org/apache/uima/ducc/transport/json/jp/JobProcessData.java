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
package org.apache.uima.ducc.transport.json.jp;

import java.util.List;

public class JobProcessData {
	
	public class LogFile {
		String name = null;
		long size = 0;
	}
	
	public long id = 0;
	public String logDirectory = null;
	public List<FileInfo> logFileList = null;
	public String hostName = null;
	public long hostPid = 0;
	public String schedulerState = null;
	public String schedulerReason = null;
	public String agentState = null;
	public String agentReason = null;
	public long timeInitStart = 0;
	public long timeInitEnd = 0;
	public long timeRunStart = 0;
	public long timeRunEnd = 0;
	public long timeGC = 0;
	public long pageIn = 0;
	public long swap = 0;
	public long swapMax = 0;
	public long rss = 0;
	public long rssMax = 0;
	public long wiTimeAvg = 0;
	public long wiTimeMax = 0;
	public long wiTimeMin = 0;
	public long wiDone = 0;
	public long wiError = 0;
	public long wiRetry = 0;
	public long wiPreempt = 0;
	public String jConsole = null;
}
