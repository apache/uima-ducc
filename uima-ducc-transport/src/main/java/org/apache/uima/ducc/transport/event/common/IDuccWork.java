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

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


public interface IDuccWork extends Identifiable, Serializable {
	
	public DuccId getDuccId();
	public void setDuccId(DuccId duccId);
	
	public String getId();
	public int getHashCode();
	
	public DuccType getDuccType();
	public void setDuccType(DuccType duccType);
	
	public IDuccStandardInfo getStandardInfo();
	public void setStandardInfo(IDuccStandardInfo standardinfo);
	
	public IDuccSchedulingInfo getSchedulingInfo();
	public void setSchedulingInfo(IDuccSchedulingInfo schedulingInfo);
	
	public Object getStateObject();
	public void setStateObject(Object state);
	
	public Object getCompletionTypeObject();
	public void setCompletionTypeObject(Object completionType);

    public String[] getServiceDependencies();
    public void setServiceDependencies(String[] serviceDependencies);

	public boolean isSchedulable();
	public boolean isCompleted();
	public boolean isOperational();
	
	public boolean isCancelOnInterrupt();
}
