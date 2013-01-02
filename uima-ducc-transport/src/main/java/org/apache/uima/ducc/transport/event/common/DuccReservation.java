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

import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.id.DuccId;

public class DuccReservation implements IDuccReservation {

	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;
	private DuccId duccId = null;
	private NodeIdentity  nodeIdentity = null;
	private int shares = 0;
	private ITimeWindow timeWindow = null;
	
	public DuccReservation(DuccId duccId, NodeIdentity nodeIdentity, int shares) {
		setDuccId(duccId);
		setNodeIdentity(nodeIdentity);
		setShares(shares);
	}
	
	@Override
	public DuccId getDuccId() {
		return duccId;
	}

	@Override
	public void setDuccId(DuccId duccId) {
		this.duccId = duccId;
	}

	@Override
	public NodeIdentity getNodeIdentity() {
		return nodeIdentity;
	}

	@Override
	public int getShares() {
		int retVal = 0;
		try {
			retVal = shares;
		}
		catch (Throwable t) {
		}
		return retVal;
	}

	@Override
	public void setShares(int shares) {
		this.shares = shares;
	}
	
	@Override
	public void setNodeIdentity(NodeIdentity nodeIdentity) {
		this.nodeIdentity = nodeIdentity;
	}

	@Override
	public ITimeWindow getTimeWindow() {
		return timeWindow;
	}

	@Override
	public void setTimeWindow(ITimeWindow timeWindow) {
		this.timeWindow = timeWindow;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((duccId == null) ? 0 : duccId.hashCode());
		result = prime * result
				+ ((nodeIdentity == null) ? 0 : nodeIdentity.hashCode());
		result = prime * result
				+ ((timeWindow == null) ? 0 : timeWindow.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DuccReservation other = (DuccReservation) obj;
		if (duccId == null) {
			if (other.duccId != null)
				return false;
		} else if (!duccId.equals(other.duccId))
			return false;
		if (nodeIdentity == null) {
			if (other.nodeIdentity != null)
				return false;
		} else if (!nodeIdentity.equals(other.nodeIdentity))
			return false;
		if (timeWindow == null) {
			if (other.timeWindow != null)
				return false;
		} else if (!timeWindow.equals(other.timeWindow))
			return false;
		return true;
	}
	
	// **********
	
//	@Override
//	public int hashCode() {
//		final int prime = 31;
//		int result = 1;
//		result = prime * result + ((getDuccId() == null) ? 0 : getDuccId().hashCode());
//		result = prime * result + ((getNodeIdentity() == null) ? 0 : getNodeIdentity().hashCode());
//		//result = prime * result + ((getTimeWindowInit() == null) ? 0 : getTimeWindowInit().hashCode());
//		//result = prime * result + ((getTimeWindowRun() == null) ? 0 : getTimeWindowRun().hashCode());
//		return result;
//	}
	
//	public boolean equals(Object obj) {
//		boolean retVal = false;
//		if(this == obj) {
//			retVal = true;
//		}
//		else if(getClass() == obj.getClass()) {
//			DuccReservation that = (DuccReservation) obj;
//			if( 	Util.compare(this.getDuccId(),that.getDuccId()) 
//				&&	Util.compare(this.getNodeIdentity(),that.getNodeIdentity()) 
//				//	These changes ignored:
//				//&&	Util.compare(this.getTimeWindowInit(),that.getTimeWindowInit()) 
//				//&&	Util.compare(this.getTimeWindowRun(),that.getTimeWindowRun())
////				&& super.equals(obj)
//				) 
//			{
//				retVal = true;
//			}
//		}
//		return retVal;
//	}

	
	
	
}
