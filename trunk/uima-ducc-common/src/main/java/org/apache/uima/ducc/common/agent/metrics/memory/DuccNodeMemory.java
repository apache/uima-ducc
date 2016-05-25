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
package org.apache.uima.ducc.common.agent.metrics.memory;

public class DuccNodeMemory implements NodeMemory {

	private static final long serialVersionUID = -6166465040486459917L;
	private long memTotal;
	private long memFree;
	private long buffers;
	private long cached;
	private long swapCached;
	private long active;
	private long inactive;
	private long swapFree;
	private long swapTotal;
	
	public long getMemTotal() {
		return memTotal;
	}

	public long getMemFree() {
		return memFree;
	}

	public long getBuffers() {
		return buffers;
	}

	public long getCached() {
		return cached;
	}

	public long getSwapCached() {
		return swapCached;
	}

	public long getActive() {
		return active;
	}

	public long getInactive() {
		return inactive;
	}

	public long getSwapTotal() {
		return swapTotal;
	}

	public long getSwapFree() {
		return swapFree;
	}

	public void setMemTotal(long memTotal) {
		this.memTotal = memTotal;
	}

	public void setMemFree(long memFree) {
		this.memFree = memFree;
	}

	public void setBuffers(long buffers) {
		this.buffers = buffers;
	}

	public void setCached(long cached) {
		this.cached = cached;
	}

	public void setSwapCached(long swapCached) {
		this.swapCached = swapCached;
	}

	public void setActive(long active) {
		this.active = active;
	}

	public void setInactive(long inactive) {
		this.inactive = inactive;
	}

	public void setSwapFree(long swapFree) {
		this.swapFree = swapFree;
	}

	public void setSwapTotal(long swapTotal) {
		this.swapTotal = swapTotal;
	}



}
