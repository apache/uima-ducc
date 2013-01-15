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

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;

/*
 * Work data
 */
public abstract class ADuccWork implements IDuccWork {

	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;
	private DuccId duccId = null;
	private DuccType duccType = DuccType.Undefined;
	private IDuccStandardInfo duccStandardInfo = null;
	private IDuccSchedulingInfo duccSchedulingInfo = null;
	private Object stateObject = null;
	private Object completionTypeObject = null;
    private String[] serviceDependencies = null;
    private ServiceDeploymentType serviceDeploymentType = null;
    private String serviceEndpoint = null; // generated in submit
	
	@Override
	public DuccId getDuccId() {
		return duccId;
	}

	@Override
	public void setDuccId(DuccId duccId) {
		this.duccId = duccId;
	}
	
	@Override
	public String getId() {
		return duccId.toString();
	}

	@Override
	public int getHashCode() {
		return duccId.hashCode();
	}
	
	@Override
	public DuccType getDuccType() {
		return duccType;
	}

	@Override
	public void setDuccType(DuccType duccType) {
		this.duccType = duccType;
	}

	@Override
	public IDuccStandardInfo getStandardInfo() {
		return duccStandardInfo;
	}

	@Override
	public void setStandardInfo(IDuccStandardInfo standardInfo) {
		this.duccStandardInfo = standardInfo;;
	}

	@Override
	public IDuccSchedulingInfo getSchedulingInfo() {
		return duccSchedulingInfo;
	}

	@Override
	public void setSchedulingInfo(IDuccSchedulingInfo schedulingInfo) {
		this.duccSchedulingInfo = schedulingInfo;
	}

	@Override
	public Object getStateObject() {
		return stateObject;
	}

	@Override
	public void setStateObject(Object state) {
		this.stateObject = state;
	}

	@Override
	public Object getCompletionTypeObject() {
		return completionTypeObject;
	}

	@Override
	public void setCompletionTypeObject(Object completionType) {
		this.completionTypeObject = completionType;
	}

    public void setServiceDependencies(String[] dependencies) {
        this.serviceDependencies = dependencies;
    }

    public String[] getServiceDependencies() {
        return this.serviceDependencies;
    }

    public void setServiceDeploymentType(ServiceDeploymentType serviceDeploymentType)
    {
        this.serviceDeploymentType = serviceDeploymentType;
    }

    public ServiceDeploymentType getServiceDeploymentType()
    {
        return serviceDeploymentType;
    }
    
    public void setServiceEndpoint(String ep)
    {
        this.serviceEndpoint = ep;
    }

    public String getServiceEndpoint()
    {
        return serviceEndpoint;
    }

	public boolean isSchedulable() {
		throw new RuntimeException("subclass must override: should never be here!");
	}
	
	public boolean isCompleted() {
		throw new RuntimeException("subclass must override: should never be here!");
	}

	
	public boolean isOperational() {
		throw new RuntimeException("subclass must override: should never be here!");
	}
	
	// **********
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getDuccId() == null) ? 0 : getDuccId().hashCode());
		result = prime * result + ((getDuccType() == null) ? 0 : getDuccType().hashCode());
		result = prime * result + ((getStandardInfo() == null) ? 0 : getStandardInfo().hashCode());
		result = prime * result + ((getSchedulingInfo() == null) ? 0 : getSchedulingInfo().hashCode());
		result = prime * result + ((getStateObject() == null) ? 0 : getStateObject().hashCode());
		result = prime * result + ((getCompletionTypeObject() == null) ? 0 : getCompletionTypeObject().hashCode());
		result = prime * result + ((getServiceEndpoint() == null) ? 0 : getServiceEndpoint().hashCode());
		result = prime * result + super.hashCode();
		return result;
	}
	
	public boolean equals(Object obj) {
		boolean retVal = false;
		if(this == obj) {
			retVal = true;
		}
		else if(getClass() == obj.getClass()) {
			ADuccWork that = (ADuccWork)obj;
			if(		Util.compare(this.getStandardInfo(),that.getStandardInfo()) 
				&&	Util.compare(this.getSchedulingInfo(),that.getSchedulingInfo()) 
				&&	Util.compare(this.getStateObject(),that.getStateObject()) 
				&&	Util.compare(this.getCompletionTypeObject(),that.getCompletionTypeObject()) 
				&&	Util.compare(this.getServiceEndpoint(),that.getServiceEndpoint()) 
//				&&	super.equals(obj)
				) 
			{
				retVal = true;
			}
		}
		return retVal;
	}
}
