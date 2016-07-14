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

import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.jd.IDriverStatusReport;

@SuppressWarnings("serial")
public class JdRequestEvent extends AbstractDuccJobEvent {
	
	private IDriverStatusReport driverStatusReport = null;
	private IDuccProcessMap processMap = null;
	
	public JdRequestEvent() {
		super(EventType.JD_STATE);
	}
	
	public IDriverStatusReport getDriverStatusReport() {
		return driverStatusReport;
	}
	
	public void setDriverStatusReport(IDriverStatusReport value) {
		driverStatusReport = value;
	}
	
	public IDuccProcessMap getProcessMap() {
		return processMap;
	}
	 
	public void setProcessMap(IDuccProcessMap value) {
		processMap = value;
	}
}
