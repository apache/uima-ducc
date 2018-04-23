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

import java.net.InetAddress;

import org.apache.uima.ducc.common.head.IDuccHead.DuccHeadState;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;

public abstract class AbstractDuccEvent 
implements DuccEvent { 

	private static final long serialVersionUID = -7200942245772203009L;

	private long sequence=0;
	
	private Long eventSize = new Long(0);
	private EventType eventType;
	
	private int cliVersion;
	
	private DuccContext context = DuccContext.Unspecified;
	
	private DuccHeadState duccHeadState = DuccHeadState.unspecified;
	private String producerHost = "unknown";
	
	public AbstractDuccEvent(EventType eventType) {
		this.eventType = eventType;
		initProducerHost();
	}
	public EventType getEventType() {
		return eventType;
	}
	public Long getEventSize() {
		return eventSize;
	}
	public void setEventSize(Long value) {
		eventSize = value;
	}
	
	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}
	
	public int getCliVersion() {
	    return cliVersion;
	}

	public void setCliVersion(int version) {
	    cliVersion = version;
	}
	
	/*
	 * Context is used to disambiguate, for example,
	 * between Service and Managed Reservation.
	 */
	public void setContext(DuccContext context) {
		this.context = context;
	}
	
	public DuccContext getContext() {
		return context;
	}
	
	/*
	 * DuccHeadState indicates Master or Backup
	 */
	public void setDuccHeadState(DuccHeadState value) {
		if(value != null) {
			duccHeadState = value;
		}
	}
	
	public DuccHeadState getDuccHeadState() {
		DuccHeadState retVal = DuccHeadState.unspecified;
		if(duccHeadState != null) {
			retVal = duccHeadState;
		}
		return retVal;
	}

	private void initProducerHost() {
		try {
			String host = InetAddress.getLocalHost().getCanonicalHostName();
			if(host != null) {
				host = host.trim();
				if(host.length() > 0) {
					producerHost = host;
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public String getProducerHost() {
		return producerHost;
	}
}
