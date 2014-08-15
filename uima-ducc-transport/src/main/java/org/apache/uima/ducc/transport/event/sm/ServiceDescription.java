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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.utils.id.ADuccId;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;



public class ServiceDescription
    implements IServiceDescription
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// For submitted and registered services
    private ArrayList<ADuccId> implementors;

    // key is job/service id, value is same.  it's a map for fast existence check
    private ArrayList<ADuccId> references;

    // UIMA-AS or CUSTOM
    private ServiceType type;

    // Implicit, Submitted, Registered
    private ServiceClass subclass;

    // for uima-as
    private String endpoint;
    private String broker;

    // The state we give OR - indicates availability of the service
    private ServiceState serviceState = ServiceState.Undefined;     
    
    // The state of the service as a DUCC job
    private JobState     jobState;

	// ping thread alive
    private boolean active;

    // current autorstart state
    private boolean autostart = true;

    // enabled?
    private boolean enabled = true;
    private String  disable_reason = null;

    // for submitted service, the registered service id
    private ADuccId id;
    private String  user;                 // the owner of the service
    private boolean deregistered;         // still known but trying to shutdown

    // number of registered instances
    private int instances;                

    private long linger;
    private Map<String, String> dependencies;

    private IServiceStatistics qstats;

    private String error_string = null;
    private long last_use = 0;

    private String registration_date = null;
    private boolean reference_start = true;

	public ADuccId getId() {
		return id;
	}

	public void setId(ADuccId id) {
		this.id = id;
	}

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

	public ArrayList<ADuccId> getImplementors() {
		return implementors;
	}

	public void setImplementors(ArrayList<ADuccId> implementors) {
		this.implementors = implementors;
	}

	public ArrayList<ADuccId> getReferences() {
		return references;
	}

	public void setReferences(ArrayList<ADuccId> references) {
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
	
    public void setQueueStatistics(IServiceStatistics qstats)
    {
        this.qstats = qstats;
    }

    public IServiceStatistics getQueueStatistics()
    {
        return qstats;
    }

    
    public boolean isAutostart() {
		return autostart;
	}

	public void setAutostart(boolean autostart) {
		this.autostart = autostart;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

    public String getDisableReason()
    {
        return disable_reason;
    }

    public void setDisableReason(String s)
    {
        this.disable_reason = s;
    }

    public void setErrorString(String s)
    {
        this.error_string = s;
    }

    public String getErrorString()
    {
        return error_string;
    }

    public void setLastUse(long l)
    {
        this.last_use = l;
    }

    public long getLastUse()
    {
        return this.last_use;
    }

    public String getLastUseString()
    {
        if ( last_use == 0) {
            return "N/A";
        }  else {
            return (new Date(last_use)).toString();
        }
    }

    public void setRegistrationDate(String s)
    {
        this.registration_date = s;
    }

    public String getRegistrationDate()
    {
        return this.registration_date;
    }

    public void setReferenceStart(boolean b)
    {
        this.reference_start = b;
    }

    public boolean isReferenceStart()
    {
        return this.reference_start;
    }

	public IServiceStatistics getQstats() {
		return qstats;
	}

	public void setQstats(IServiceStatistics qstats) {
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

        sb.append("   Service Class     : ");
        sb.append(subclass.decode());
        switch ( subclass ) {
            case Registered:
                sb.append(" as ID ");
                sb.append(id);
                sb.append(" Owner[");
                sb.append(user);
                sb.append("] instances[");
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

        sb.append("   Implementors      : ");
        if ( implementors.size() > 0 ) {
            for (ADuccId id : implementors) {
                sb.append(id);
                sb.append(" ");
            }
        } else {
            sb.append("(N/A)");
        }
        sb.append("\n");

        sb.append("   References        : ");
        if ( references.size() > 0 ) {
            for ( ADuccId id : references ) {
                sb.append(id);
                sb.append(" ");
            }
        } else {
            sb.append("None");
        }
        sb.append("\n");

        sb.append("   Dependencies      : ");
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

        sb.append("   Service State     : ");
        sb.append(serviceState);
        sb.append("\n");

        sb.append("   Ping Active       : ");
        sb.append(active);
        sb.append("\n");

        sb.append("   Start Mode        : ");
        if ( autostart )            { sb.append("autostart"); }
        else if ( reference_start ) { sb.append("reference"); }
        else                        { sb.append("manual"); }
        
        if ( enabled ) {
            sb.append(", Enabled");
        } else {
            sb.append(", Disabled; reason: ");
            sb.append(disable_reason);
        }
        sb.append("\n");

        sb.append("   Last Use          : ");
        sb.append(getLastUseString());
        sb.append("\n");

        sb.append("   Registration Date : ");
        sb.append(registration_date);
        sb.append("\n");

        if ( error_string != null ) {
            sb.append("   Errors       : ");
            sb.append(error_string);
            sb.append("\n");
        }

        sb.append("   Service Statistics: ");
        if ( qstats == null ) {
            sb.append("None\n");
        } else {
            sb.append("\n       ");            
            sb.append(qstats.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

}
