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
package org.apache.uima.ducc.agent.launcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class ProcessStreamConsumer extends Thread {
	public static final String LS = System.getProperty("line.separator");

	private InputStream is;
	private PrintStream os;
	DuccLogger logger;
	private DuccId workDuccId;
	
	private StringBuffer errStreamBuffer = new StringBuffer();
	public ProcessStreamConsumer(DuccLogger logger, ThreadGroup threadGroup,
				final String threadName, InputStream is, PrintStream os, DuccId workDuccId) {
		super(threadGroup, threadName);
		this.os = os;
		this.is = is;
		this.logger = logger;
		setDaemon(true);
		this.workDuccId = workDuccId;
	}
  public String getDataFromStream() {
    return errStreamBuffer.toString();
  }
	public void run() {
		try {

			InputStreamReader in = new InputStreamReader(is);
			BufferedReader reader = new BufferedReader(in);
			String line;
			while ((line = reader.readLine()) != null) {
				line += LS;
				if ( "StdErrorReader".equals(Thread.currentThread().getName())) {
					logger.error("ProcessStreamConsumer.run()", workDuccId, line.trim());
					os.print("ERR>>>"+line);
					// Save stderr stream into a local buffer. When the process exits unexpectedly
					// errors will be copied to a Process object.
					errStreamBuffer.append(line.trim());  
				} else {
					logger.info("ProcessStreamConsumer.run()", workDuccId, line.trim());
					os.print("OUT>>>"+line);
				}
				// Check if duccling redirected its output streams to a log. If so, it would put
				// out a marker that starts with "1200 Redirecting stdout". This is a clue to
				// stop consuming from the process streams. Just close streams and
				// return.
				if (line.trim().startsWith("1200 Redirecting stdout")) {
					break;
				}
			}
		} catch (Exception x) {
		} finally {
			try {
				os.flush();
			} catch (Exception e) {
				System.out.println("Caught Exception While Flushing "
						+ Thread.currentThread().getName() + " Output Stream");
			}
			try {
				if (is != null) {
					is.close();
				}
			} catch (IOException e) {
				// ignore
			}
		}
	}
}
