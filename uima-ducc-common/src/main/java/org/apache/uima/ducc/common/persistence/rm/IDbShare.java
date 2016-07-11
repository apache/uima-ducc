package org.apache.uima.ducc.common.persistence.rm;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.id.DuccId;

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
public interface IDbShare {


	public abstract int getNodepoolDepth();

	public abstract String getNodepoolId();

	public abstract DuccId getId();

	// UIMA-4142
	public abstract boolean isBlacklisted();

	// UIMA-4142
	public abstract DuccId getBlJobId();

	public abstract NodeIdentity getNodeIdentity();

	public abstract Node getNode();

	/**
	 * The order of the share itself.
	 */
	public abstract int getShareOrder();

	/**
	 * Returns only initialization time.  Eventually getInvestment() may take other things into
	 * consideration so we separate these two (even though currently they do the same thing.)
	 */
	public abstract long getInitializationTime();

	public abstract void setInitializationTime(long millis);

	public abstract void setFixed();

	public abstract boolean isPurged();
    public abstract boolean isEvicted();
    public abstract boolean isFixed();

}
