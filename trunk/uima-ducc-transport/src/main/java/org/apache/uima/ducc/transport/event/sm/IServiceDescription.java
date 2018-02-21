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

import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;

/**
 * This interface defines the information returned in response to service query events.
 *
 * All the 'set' methods are used only by the ServiceManager; they have useful no effect if
 * invoked in any other context.
 *
 */

public interface IServiceDescription
    extends IService
{

    /**
     * @return the Unique ID of the service as assigned by DUCC.
     */
	public Long getId();
    /*
     * Internal to DUCC.
     */
	public void setId(Long id);

    /**
     * @return the owner of the service
     */
    public String getUser();
    /*
     * Set the owner of the service
     */
    public void   setUser(String u);

    /**
     * This returns the set of DUCC Ids for all the service instances which 
     * implement this service.  If the list is empty, no known implementors of the service
     * are being managed by DUCC.
     *
     * @return List of DUCC Ids of service implementors.  Parallel array with getInstanceIds().
     */
	public Long[] getImplementors();

    /**
     * This returns the list of contant instance ids for multi-instance services.
     *
     * @return List of service instance ids.  Parallel array with getImplementors().
     */
	public Integer[] getInstanceIds();

    /*
     * Internal to DUCC.
     */
	public void setImplementors(ArrayList<Long> implementors, ArrayList<Integer> instancids);

	public Long[] getReferences();
    /*
     * Internal to DUCC.
     */
	public void setReferences(ArrayList<Long> references);

	public ServiceType getType();
    /*
     * Internal to DUCC.
     */
	public void setType(ServiceType type);

	public ServiceClass getSubclass();
    /*
     * Internal to DUCC.
     */
	public void setSubclass(ServiceClass subclass);

	public String getEndpoint();
    /*
     * Internal to DUCC.
     */
	public void setEndpoint(String endpoint);

	public String getBroker();
    /*
     * Internal to DUCC.
     */
	public void setBroker(String broker);

	public ServiceState getServiceState();
    /*
     * Internal to DUCC.
     */
	public void setServiceState(ServiceState serviceState);

	public JobState getJobState();
    /*
     * Internal to DUCC.
     */
	public void setJobState(JobState jobState);

	public boolean isActive();
    /*
     * Internal to DUCC.
     */
	public void setActive(boolean active);

	public void setDeregistered(boolean d);	
    /*
     * Internal to DUCC.
     */
    public void setQueueStatistics(IServiceStatistics qstats);    

    public IServiceStatistics getQueueStatistics();
    /*
     * Internal to DUCC.
     */
    public void setAutostart(boolean autostart);
    public boolean isAutostart();

    public void setReferenceStart(boolean ref);
    public boolean isReferenceStart();

	public boolean isEnabled();
    public void   setDisableReason(String r);
    public String getDisableReason();

    public void setLastUse(long last_use);
    public long getLastUse();
    public String getLastUseString();

    // UIMA-4309
    public void setLastPing(long last_ping);
    public long getLastPing();
    public String getLastPingString();

    // UIMA-4309
    public void setLastRunnable(long last_runnable);
    public long getLastRunnable();
    public String getLastRunnableString();

    public void setRegistrationDate(String d);
    public String getRegistrationDate();

    public String getErrorString();
    public void   setErrorString(String s);

    /*
     * Internal to DUCC.
     */
	public void setEnabled(boolean enable);

	public IServiceStatistics getQstats();
    /*
     * Internal to DUCC.
     */
	public void setQstats(IServiceStatistics qstats);

	public boolean isDeregistered();

	public void setInstances(int instances);
    /*
     * Internal to DUCC.
     */
    public int getInstances();

    public void setLinger(long linger);    
    /*
     * Internal to DUCC.
     */
    public long getLinger();    

    /*
     * Internal to DUCC.
     */
    public void addDependency(String endpoint, String msg);
    public Map<String, String> getDependencies();

}
