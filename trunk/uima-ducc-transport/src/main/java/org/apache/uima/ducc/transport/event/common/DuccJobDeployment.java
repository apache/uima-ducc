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
package org.apache.uima.ducc.transport.event.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.cmdline.JavaCommandLine;
import org.apache.uima.ducc.transport.cmdline.NonJavaCommandLine;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;

public class DuccJobDeployment implements IDuccJobDeployment {
	private static final long serialVersionUID = 1L;
	private DuccId jobId;
	// at most two command lines can be accommodated
	private ICommandLine[] jdclArray = new JavaCommandLine[1];
	private ICommandLine[] pclArray;// = new JavaCommandLine[2];

	private IDuccStandardInfo stdInfo;
	private List<IDuccProcess> jobProcesses = new ArrayList<IDuccProcess>();
//	private long processMemoryAssignment;
	private ProcessMemoryAssignment pma; 
	private DuccType type;
	
	public DuccJobDeployment(DuccId jobId, ICommandLine jdCmdLine,
			ICommandLine jpCmdLine, IDuccStandardInfo stdInfo,
			IDuccProcess jdProcess, ProcessMemoryAssignment pma,
			List<IDuccProcess> jps, DuccType type) {
		this.jobId = jobId;
		// this.jdclArray = new JavaCommandLine[2];

		if (jpCmdLine instanceof JavaCommandLine) {
			this.pclArray = new JavaCommandLine[1];
		} else {
			this.pclArray = new NonJavaCommandLine[1];
		}
		this.jdclArray[0] = jdCmdLine;
		this.pclArray[0] = jpCmdLine;
		this.stdInfo = stdInfo;
		this.jobProcesses.add(jdProcess);
		this.jobProcesses.addAll(jps);
		this.pma = pma;
		this.type =type;
		//this.processMemoryAssignment = processMemoryAssignment;
	}

	public DuccType getType() {
		return type;
	}

	public ICommandLine getJdCmdLine() {
		return this.jdclArray[0];
	}

	public ICommandLine getJpCmdLine() {
		return this.pclArray[0];
	}

	public IDuccStandardInfo getStandardInfo() {
		return this.stdInfo;
	}

	public IDuccProcess getJdProcess() {
		return this.jobProcesses.get(0);
	}

	public List<IDuccProcess> getJpProcessList() {
		return this.jobProcesses.subList(1, this.jobProcesses.size());
	}

	public DuccId getJobId() {
		return jobId;
	}

	public ProcessMemoryAssignment getProcessMemoryAssignment() {
		return pma;
	}
}
