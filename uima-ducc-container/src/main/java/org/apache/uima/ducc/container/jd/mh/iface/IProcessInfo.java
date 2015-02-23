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

import org.apache.uima.ducc.container.jd.mh.iface.remote.IRemotePid;

public interface IProcessInfo extends IRemotePid, Serializable{
	
	public void setDispatch(long value);
	public long getDispatch();
	
	public void setDone(long value);
	public long getDone();
	
	public void setError(long value);
	public long getError();
	
	public void setPreempt(long value);
	public long getPreempt();
	
	public void setRetry(long value);
	public long getRetry();
	
	public void setAvg(long value);
	public long getAvg();
	
	public void setMax(long value);
	public long getMax();
	
	public void setMin(long value);
	public long getMin();
	
	public void setReasonStopped(String value);
	public String getReasonStopped();
	
	public void setReasonDeallocated(String value);
	public String getReasonDeallocated();
}
