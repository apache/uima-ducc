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
import java.io.InputStreamReader;

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;
import org.apache.uima.ducc.transport.event.common.DuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;

public class DefunctProcessDetector implements Runnable {
	ManagedProcess childProcess;
	DuccLogger logger;
	public DefunctProcessDetector(ManagedProcess process, DuccLogger logger) {
		childProcess = process;
		this.logger = logger;
	}
	private  boolean isDefunctProcess(String pid) throws Exception {
		boolean zombie = false;
		InputStreamReader in = null;
		String[] command = {
				"/bin/ps",
				"-ef",
				pid};
		try {
			ProcessBuilder pb = new ProcessBuilder();
			pb.command(command);

			pb.redirectErrorStream(true);
			java.lang.Process childProcess = pb.start();
			in = new InputStreamReader(childProcess.getInputStream());
			BufferedReader reader = new BufferedReader(in);
			String line = null;
			while ((line = reader.readLine()) != null) {
				// the ps line will have string <defunct> if the 
				// process is a zombie
				zombie = (line.indexOf("defunct") > 0);
				if ( zombie ) {
   				   logger.info("DefunctProcessDetector.isDefunctProcess", null, "Process with PID:"+pid+" Is Defunct - OS reports:"+line);
				    break;
				}
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (Exception e) {
				}
			}
		}
		return zombie;
	}

	public void run() {
		try {
			if (isDefunctProcess(childProcess.getPid())) {
				logger.info("DefunctProcessDetector.run()", childProcess.getDuccProcess().getDuccId(), "Process with PID:"+childProcess.getPid()+" Is Defunct - Changing State to Stopped");
				childProcess.getDuccProcess().setProcessState(ProcessState.Stopped);
				childProcess.getDuccProcess().setReasonForStoppingProcess(ReasonForStoppingProcess.Defunct.name());
			} else {
				logger.debug("DefunctProcessDetector.run()", childProcess.getDuccProcess().getDuccId(), "Process with PID:"+childProcess.getPid()+" Not Defunct");

			}
		} catch( Exception e) {
			logger.error("DefunctProcessDetector.run()", childProcess.getDuccProcess().getDuccId(), e);
		}
		
	}
	public static void main(String[] args) {
		try {
			DuccLogger logger = new DuccLogger(DefunctProcessDetector.class);
			DuccIdFactory factory = new DuccIdFactory();
			DuccId duccId = factory.next();
			NodeIdentity nid = new NodeIdentity();
			IDuccProcess process = new DuccProcess(duccId, nid);
			ManagedProcess p = new ManagedProcess(process, null);
			process.setProcessState(ProcessState.Initializing);
			process.setPID(args[0]);
			p.setPid(args[0]);
			DefunctProcessDetector detector = new DefunctProcessDetector(p, logger);
			detector.run();
		} catch( Exception e) {
			e.printStackTrace();
		}
	}

}
