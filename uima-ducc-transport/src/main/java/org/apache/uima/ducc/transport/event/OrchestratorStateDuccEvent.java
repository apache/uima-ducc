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
package org.apache.uima.ducc.transport.event;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkExecutable;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;

public class OrchestratorStateDuccEvent extends AbstractDuccEvent  {
	
	private static final long serialVersionUID = 2L;
	
	private static DuccId jobid = null;
	private static DuccLogger logger = null;
	
	private IDuccWorkMap workMap = null;

	public OrchestratorStateDuccEvent() {
		super(EventType.ORCHESTRATOR_STATE);
	}
	
	public OrchestratorStateDuccEvent(DuccLogger duccLogger) {
		super(EventType.ORCHESTRATOR_STATE);
		logger = duccLogger;
	}
	
	public void setWorkMap(IDuccWorkMap value) {
		this.workMap = value.deepCopy();
		trim();
	}
	
	public IDuccWorkMap getWorkMap() {
		IDuccWorkMap value = this.workMap.deepCopy();
		return value;
	}
	
	private int sizeOf(Object object) {
		String location = "sizeOf";
		int retVal = 0;
		try {
			if(object != null) {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
			    ObjectOutputStream os = new ObjectOutputStream(bos);
			    os.writeObject(object);
			    String string = bos.toString();
			    os.close();
			    retVal = string.length();
			}
		}
		catch(Exception e) {
			if(logger != null) {
				logger.error(location, jobid, e);
			}
			else {
				e.printStackTrace();
			}
		}
		return retVal;
	}
	
	private void trim() {
		String location = "trim";
		int bytesTrimmed = 0;
		for(Object key : workMap.keySet()) {
			DuccId duccId = (DuccId) key;
			IDuccWork dw = (IDuccWork) workMap.get(duccId);
			if(dw instanceof IDuccWorkJob) {
				IDuccWorkJob job = (IDuccWorkJob) dw;
				if(logger != null) {
					int s1 = sizeOf(job.getCommandLine());
					int s2 = sizeOf(job.getDriver().getCommandLine());
					bytesTrimmed = bytesTrimmed+(s1+s2);
					String message = "jd:"+s1+" jp:"+s2+" total:"+bytesTrimmed;
					logger.trace(location, duccId, message);
				}
				job.setCommandLine(null);
				job.getDriver().setCommandLine(null);
			}
			else if(dw instanceof IDuccWorkService) {
				IDuccWorkJob service = (IDuccWorkJob) dw;
				if(logger != null) {
					int s1 = 0;
					int s2 = sizeOf(service.getCommandLine());
					bytesTrimmed = bytesTrimmed+(s1+s2);
					String message = "sp:"+s2+" total:"+bytesTrimmed;
					logger.trace(location, duccId, message);
				}
				service.setCommandLine(null);
			}
			else if(dw instanceof IDuccWorkExecutable) {
				IDuccWorkExecutable dwe = (IDuccWorkExecutable) dw;
				if(logger != null) {
					int s1 = sizeOf(dwe.getCommandLine());
					int s2 = 0;
					bytesTrimmed = bytesTrimmed+(s1+s2);
					String message = "mr:"+s1+" total:"+bytesTrimmed;
					logger.trace(location, duccId, message);
				}
				dwe.setCommandLine(null);
			}
		}
		if(logger != null) {
			String message = "total:"+bytesTrimmed;
			logger.debug(location, jobid, message);
		}
	}
}
