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
package org.apache.uima.ducc.container.jd.mh.iface;

import java.io.Serializable;

import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteNode;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemotePid;
import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemoteTid;

public interface IWorkItemInfo extends IRemoteNode, IRemotePid, IRemoteTid, Serializable {
	
	public int getSeqNo();
	public void setSeqNo(int value);
	
	public long getOperatingMillis();
	public void setOperatingMillis(long value);
	
	public long getInvestmentMillis();
	public void setInvestmentMillis(long value);
}
