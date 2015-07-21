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

import org.apache.uima.ducc.transport.cmdline.ICommandLine;

public class ADuccWorkExecutable extends ADuccWork implements IDuccWorkExecutable {

	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;
	private IDuccProcessMap processMap = new DuccProcessConcurrentMap();
	private ICommandLine commandLine = null;
//	private IDuccUimaAggregate uimaAggregate = null;
	private IDuccUimaDeployableConfiguration uimaDeployableConfiguration = null;
	
	
	public IDuccProcessMap getProcessMap() {
		return processMap;
	}

	
	public void setProcessMap(IDuccProcessMap processMap) {
		this.processMap = processMap;
	}

	
	public ICommandLine getCommandLine() {
		return commandLine;
	}

	
	public void setCommandLine(ICommandLine commandLine) {
		this.commandLine = commandLine;
	}

//	
//	public IDuccUimaAggregate getUimaAggregate() {
//		return uimaAggregate;
//	}
//
//	
//	public void setUimaAggregate(IDuccUimaAggregate uimaAggregate) {
//		this.uimaAggregate = uimaAggregate;
//	}
	
	// **********
	
	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getProcessMap() == null) ? 0 : getProcessMap().hashCode());
		result = prime * result + ((getCommandLine() == null) ? 0 : getCommandLine().hashCode());
		result = prime * result + ((getUimaDeployableConfiguration() == null) ? 0 : getUimaDeployableConfiguration().hashCode());
		result = prime * result + super.hashCode();
		return result;
	}
	
	public boolean equals(Object obj) {
		boolean retVal = false;
		if(this == obj) {
			retVal = true;
		}
		else if(getClass() == obj.getClass()) {
			ADuccWorkExecutable that = (ADuccWorkExecutable)obj;
			if( 	Util.compare(this.getProcessMap(),that.getProcessMap()) 
				&&	Util.compare(this.getCommandLine(),that.getCommandLine()) 
				&&	Util.compare(this.getUimaDeployableConfiguration(),that.getUimaDeployableConfiguration()) 
//				&&	super.equals(obj)
				) 
			{
				retVal = true;
			}
		}
		
		
		return retVal;
	}

	public IDuccUimaDeployableConfiguration getUimaDeployableConfiguration() {
		return uimaDeployableConfiguration;
	}

	public void setUimaDeployableConfiguration(
			IDuccUimaDeployableConfiguration uimaDeployableConfiguration) {
		this.uimaDeployableConfiguration = uimaDeployableConfiguration;
	}

}
