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
package org.apache.uima.ducc.container.jd.mh.impl;

import org.apache.uima.ducc.container.jd.mh.iface.IOperatingInfo;

public class OperatingInfo implements IOperatingInfo {

	private int crTotal = 0;
	private int crFetches = 0;
	private int jpSends = 0;
	private int jpAcks = 0;
	private int jpEndSuccesses = 0;
	private int jpEndFailures = 0;
	private int jpUserProcessingTimeouts = 0;
	private int jpUserProcessingErrorRetries = 0;
	
	@Override
	public void setWorkItemCrTotal(int value) {
		crTotal = value;
	}

	@Override
	public int getWorkItemCrTotal() {
		return crTotal;
	}

	@Override
	public void setWorkItemCrFetches(int value) {
		crFetches = value;
	}

	@Override
	public int getWorkItemCrFetches() {
		return crFetches;
	}

	@Override
	public void setWorkItemJpSends(int value) {
		jpSends = value;
	}

	@Override
	public int getWorkItemJpSends() {
		return jpSends;
	}

	@Override
	public void setWorkItemJpAcks(int value) {
		jpAcks = value;
	}

	@Override
	public int getWorkItemJpAcks() {
		return jpAcks;
	}

	@Override
	public void setWorkItemEndSuccesses(int value) {
		jpEndSuccesses = value;
	}

	@Override
	public int getWorkItemEndSuccesses() {
		return jpEndSuccesses;
	}

	@Override
	public void setWorkItemEndFailures(int value) {
		jpEndFailures = value;
	}

	@Override
	public int getWorkItemEndFailures() {
		return jpEndFailures;
	}

	@Override
	public void setWorkItemUserProcessingTimeouts(int value) {
		jpUserProcessingTimeouts = value;
	}

	@Override
	public int getWorkItemUserProcessingTimeouts() {
		return jpUserProcessingTimeouts;
	}

	@Override
	public void setWorkItemUserProcessingErrorRetries(int value) {
		jpUserProcessingErrorRetries = value;
	}

	@Override
	public int getWorkItemUserProcessingErrorRetries() {
		return jpUserProcessingErrorRetries;
	}

	
}
