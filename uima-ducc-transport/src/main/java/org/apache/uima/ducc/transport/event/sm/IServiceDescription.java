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
package org.apache.uima.ducc.transport.event.sm;

import java.util.ArrayList;
import java.util.Map;

import org.apache.uima.ducc.common.ServiceStatistics;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;

public interface IServiceDescription
    extends IService
{

	public DuccId getId();
	public void setId(DuccId id);

	public ArrayList<DuccId> getImplementors();
	public void setImplementors(ArrayList<DuccId> implementors);

	public ArrayList<DuccId> getReferences();
	public void setReferences(ArrayList<DuccId> references);

	public ServiceType getType();
	public void setType(ServiceType type);

	public ServiceClass getSubclass();
	public void setSubclass(ServiceClass subclass);

	public String getEndpoint();
	public void setEndpoint(String endpoint);

	public String getBroker();
	public void setBroker(String broker);

	public ServiceState getServiceState();
	public void setServiceState(ServiceState serviceState);

	public JobState getJobState();
	public void setJobState(JobState jobState);

	public boolean isActive();
	public void setActive(boolean active);

	public void setDeregistered(boolean d);	
    public void setQueueStatistics(ServiceStatistics qstats);    

    public ServiceStatistics getQueueStatistics();
    public void setAutostart(boolean autostart);

	public boolean isStopped();
	public void setStopped(boolean stopped);

	public ServiceStatistics getQstats();
	public void setQstats(ServiceStatistics qstats);

	public boolean isDeregistered();

	public void setInstances(int instances);
    public int getInstances();

    public void setLinger(long linger);    
    public long getLinger();    

    public void addDependency(String endpoint, String msg);
    public Map<String, String> getDependencies();

}
