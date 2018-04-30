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
package org.apache.uima.ducc.ps.net.impl;

import org.apache.uima.ducc.ps.net.iface.IMetaTask;

public class MetaTask implements IMetaTask {

	private static final long serialVersionUID = 1L;
	
	private String systemKey = (new Integer(-1)).toString();
	private String userKey = null;
	private String performanceMetrics = null;
	private Object userSpaceTask = null;
	private Object userSpaceException = null;
    private String appData = null;
	/////
	
	public MetaTask(int seqNo, String documentText, Object userSpaceTask) {
		setSeqNo(seqNo);
		setDocumentText(documentText);
		setUserSpaceTask(userSpaceTask);
	}
	public int getSeqNo() {
		return Integer.parseInt(getSystemKey());
	}
	public String getAppData() {
		return appData;
	}
	public void setAppData(String value) {
		appData = value;
	}

	public void setSeqNo(int value) {
		setSystemKey(Integer.toString(value));
	}
	
	public String getDocumentText() {
		return getUserKey();
	}
	
	public void setDocumentText(String value) {
		setUserKey(value);
	}
	
	public String getSerializedTask() {
		return (String)getUserSpaceTask();
	}
	
	public void setSerializedTask(String value) {
		setUserSpaceTask(value);
	}
	
	/////
	
	@Override
	public String getSystemKey() {
		return systemKey;
	}

	@Override
	public void setSystemKey(String value) {
		systemKey = value;
	}

	@Override
	public String getUserKey() {
		return userKey;
	}

	@Override
	public void setUserKey(String value) {
		userKey = value;
	}

	@Override
	public String getPerformanceMetrics() {
		return performanceMetrics;
	}

	@Override
	public void setPerformanceMetrics(String serializedMetrics) {
		performanceMetrics = serializedMetrics;
	}

	@Override
	public Object getUserSpaceTask() {
		return userSpaceTask;
	}

	@Override
	public void setUserSpaceTask(Object value) {
		userSpaceTask = value;
	}

	@Override
	public Object getUserSpaceException() {
		return userSpaceException;
	}

	@Override
	public void setUserSpaceException(Object value) {
		userSpaceException = value;
	}

	
}
