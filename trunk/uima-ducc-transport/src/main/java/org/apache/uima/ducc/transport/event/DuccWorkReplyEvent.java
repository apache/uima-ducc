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

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccWork;

public class DuccWorkReplyEvent extends AbstractDuccOrchestratorEvent {

	private static final long serialVersionUID = 1L;
	
	private IDuccWork dw = null;
	
	public DuccWorkReplyEvent() {
		super(EventType.DUCCWORK);
		setDw(dw);
	}
	
	public void setDw(IDuccWork value) {
		dw = value;
	}
	
	public IDuccWork getDw() {
		return dw;
	}
	
	public DuccId getDuccId() {
		DuccId retVal = null;
		if(dw != null) {
			retVal = dw.getDuccId();
		}
		return retVal;
	}
	
}
