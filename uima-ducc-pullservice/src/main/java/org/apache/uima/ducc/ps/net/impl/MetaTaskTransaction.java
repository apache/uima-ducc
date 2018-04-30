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

import java.util.List;

import org.apache.uima.ducc.ps.net.iface.IMetaTask;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;


public class MetaTaskTransaction implements IMetaTaskTransaction {

	private static final long serialVersionUID = 1L;

	private TransactionId transactionId = null;
	
	private Type type = null;
	private Direction direction = null;
	
	private List<Hint> hints = null;
	
	private String providerKey = null;
	private String providerName = null;
	private int providerPort = 0;
	
	private String requesterNodeName = null;
	private String requesterAddress = null;
	private String requesterPidName = null;
	private int requesterPid = 0;
	private int requesterTid = 0;
	
	private IMetaTask metaTask = null;
	
	private JdState jdState = null;
	
	public MetaTaskTransaction() {
		transactionId = new TransactionId(0,0);
	}
	
	@Override
	public String getProviderKey() {
		return providerKey;
	}
	
	@Override
	public void setProviderKey(String value) {
		providerKey = value;
	}
	
	@Override
	public String getProviderName() {
		return providerName;
	}

	@Override
	public void setProviderName(String value) {
		providerName = value;
	}

	@Override
	public int getProviderPort() {
		return providerPort;
	}

	@Override
	public void setProviderPort(int value) {
		providerPort = value;
	}

	@Override
	public String getRequesterNodeName() {
		return requesterNodeName;
	}

	@Override
	public void setRequesterNodeName(String value) {
		requesterNodeName = value;
	}

	@Override
	public String getRequesterAddress() {
		return requesterAddress;
	}

	@Override
	public void setRequesterAddress(String value) {
		requesterAddress = value;
	}

	@Override
	public String getRequesterProcessName() {
		return requesterPidName;
	}

	@Override
	public void setRequesterProcessName(String value) {
		requesterPidName = value;
	}
	
	@Override
	public int getRequesterProcessId() {
		return requesterPid;
	}

	@Override
	public void setRequesterProcessId(int value) {
		requesterPid = value;
	}

	@Override
	public int getRequesterThreadId() {
		return requesterTid;
	}

	@Override
	public void setRequesterThreadId(int value) {
		requesterTid = value;
	}

	@Override
	public Type getType() {
		return type;
	}

	@Override
	public void setType(Type value) {
		type = value;
	}

	@Override
	public Direction getDirection() {
		return direction;
	}

	@Override
	public void setDirection(Direction value) {
		direction = value;
	}

	@Override
	public List<Hint> getResponseHints() {
		return hints;
	}

	@Override
	public void setResponseHints(List<Hint> value) {
		hints = value;
	}

	@Override
	public TransactionId getTransactionId() {
		return transactionId;
	}

	@Override
	public void setTransactionId(TransactionId value) {
		transactionId = value;
	}

	@Override
	public IMetaTask getMetaTask() {
		return metaTask;
	}

	@Override
	public void setMetaTask(IMetaTask value) {
		metaTask = value;
	}

	@Override
	public JdState getJdState() {
		return jdState;
	}

	@Override
	public void setJdState(JdState value) {
		jdState = value;
	}

}
