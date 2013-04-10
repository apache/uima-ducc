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

import java.io.Serializable;

import org.apache.uima.ducc.common.utils.id.IDuccId;

public class CGroup implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private IDuccId id;
	private long maxMemoryLimit;  // in bytes
	private boolean reservation;
	private int shares;
	
	public CGroup(long max_size_in_bytes) {
		setMaxMemoryLimit(max_size_in_bytes);
	}
	
	public int getShares() {
		return shares;
	}
	public void setShares(int shares) {
		this.shares = shares;
	}
	public boolean isReservation() {
		return reservation;
	}
	public void setReservation(boolean reservation) {
		this.reservation = reservation;
	}
	public IDuccId getId() {
		return id;
	}
	public void setId(IDuccId id) {
		this.id = id;
	}
	public long getMaxMemoryLimit() {
		return maxMemoryLimit;
	}
	public void setMaxMemoryLimit(long maxMemoryLimit) {
		this.maxMemoryLimit = maxMemoryLimit;
	}
	
}
