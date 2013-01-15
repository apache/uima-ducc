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
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.common.ServiceStatistics;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;



public class ServiceDescription
    implements IServiceDescription
{
    // For submitted and registered services
    ArrayList<DuccId> implementors;

    // key is job/service id, value is same.  it's a map for fast existence check
    ArrayList<DuccId> references;

    // UIMA-AS or CUSTOM
    ServiceType type;

    // Implicit, Submitted, Registered
    ServiceClass subclass;

    // for uima-as
    String endpoint;
    String broker;

    // The state we give OR - indicates availability of the service
    ServiceState serviceState = ServiceState.Undefined;     
    
    // The state of the service as a DUCC job
    JobState     jobState;

	// ping thread alive
    boolean active;

    // current autorstart state
    boolean autostart = true;

    // manual stop?
    boolean stopped = false;

    // for submitted service, the registered service id
    DuccId id;
    boolean deregistered;         // still known but trying to shutdown

    // number of registered instances
    int instances;                

    long linger;
    Map<String, String> dependencies;

    ServiceStatistics qstats;

	public DuccId getId() {
		return id;
	}

	public void setId(DuccId id) {
		this.id = id;
	}

	public ArrayList<DuccId> getImplementors() {
		return implementors;
	}

	public void setImplementors(ArrayList<DuccId> implementors) {
		this.implementors = implementors;
	}

	public ArrayList<DuccId> getReferences() {
		return references;
	}

	public void setReferences(ArrayList<DuccId> references) {
		this.references = references;
	}

	public ServiceType getType() {
		return type;
	}

	public void setType(ServiceType type) {
		this.type = type;
	}

	public ServiceClass getSubclass() {
		return subclass;
	}

	public void setSubclass(ServiceClass subclass) {
		this.subclass = subclass;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public String getBroker() {
		return broker;
	}

	public void setBroker(String broker) {
		this.broker = broker;
	}

	public ServiceState getServiceState() {
		return serviceState;
	}

	public void setServiceState(ServiceState serviceState) {
		this.serviceState = serviceState;
	}

	public JobState getJobState() {
		return jobState;
	}

	public void setJobState(JobState jobState) {
		this.jobState = jobState;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public void setDeregistered(boolean d)
	{
		this.deregistered = d;
	}
	
    public void setQueueStatistics(ServiceStatistics qstats)
    {
        this.qstats = qstats;
    }

    public ServiceStatistics getQueueStatistics()
    {
        return qstats;
    }

    
    public boolean isAutostart() {
		return autostart;
	}

	public void setAutostart(boolean autostart) {
		this.autostart = autostart;
	}

	public boolean isStopped() {
		return stopped;
	}

	public void setStopped(boolean stopped) {
		this.stopped = stopped;
	}

	public ServiceStatistics getQstats() {
		return qstats;
	}

	public void setQstats(ServiceStatistics qstats) {
		this.qstats = qstats;
	}

	public boolean isDeregistered() {
		return deregistered;
	}

	public void setInstances(int instances)
    {
        this.instances = instances;
    }

    public int getInstances()
    {
        return instances;
    }

    public void setLinger(long linger)
    {
    	this.linger = linger;
    }
    
    public long getLinger()
    {
    	return this.linger;
    }
    
    public void addDependency(String endpoint, String msg)
    {
        if ( this.dependencies == null ) {
            this.dependencies = new HashMap<String, String>();
        }
        this.dependencies.put(endpoint, msg);
    }

    public Map<String, String> getDependencies()
    {
    		return this.dependencies;
    }
    
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("Service: ");
        sb.append(type.decode());
        sb.append(":");
        sb.append(endpoint);

        if ( type == ServiceType.UimaAs ) {
            sb.append(":");
            sb.append(broker);
        }
        sb.append("\n");

        sb.append("   Service Class   : ");
        sb.append(subclass.encode());
        switch ( subclass ) {
            case Registered:
                sb.append(" as ID ");
                sb.append(id);
                sb.append(" instances[");
                sb.append(Integer.toString(instances));
                sb.append("] linger[");
                sb.append(Long.toString(linger));
                sb.append("]");
                break;
            case Submitted:
            case Implicit:
            default:
        }
        sb.append("\n");

        sb.append("   Implementors    : ");
        if ( implementors.size() > 0 ) {
            for (DuccId id : implementors) {
                sb.append(id);
                sb.append(" ");
            }
        } else {
            sb.append("(N/A)");
        }
        sb.append("\n");

        sb.append("   References      : ");
        if ( references.size() > 0 ) {
            for ( DuccId id : references ) {
                sb.append(id);
                sb.append(" ");
            }
        } else {
            sb.append("None");
        }
        sb.append("\n");

        sb.append("   Dependencies    : ");
        if ( dependencies == null ) {
            sb.append("none\n");
        } else {
            sb.append("\n");
            for ( String s : dependencies.keySet() ) {
                sb.append("      ");
                sb.append(s);
                sb.append(": ");
                sb.append(dependencies.get(s));
                sb.append("\n");
            }
        }

        sb.append("   Service State   : ");
        sb.append(serviceState);
        sb.append("\n");

        sb.append("   Ping Active     : ");
        sb.append(active);
        sb.append("\n");

        sb.append("   Autostart       : ");
        sb.append(autostart);
        sb.append("\n");
        
        sb.append("   Manual Stop     : ");
        sb.append(stopped);
        sb.append("\n");

        sb.append("   Service Statistics: ");
        if ( qstats == null ) {
            sb.append("None\n");
        } else {
            sb.append("\n       ");            
            sb.append(qstats.toString());
        }
        return sb.toString();
    }

}
