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
package org.apache.uima.ducc.container.jd.wi.perf;

import org.apache.uima.ducc.container.common.MessageBuffer;
import org.apache.uima.ducc.container.common.Standardize;
import org.apache.uima.ducc.container.common.logger.IComponent;
import org.apache.uima.ducc.container.common.logger.ILogger;
import org.apache.uima.ducc.container.common.logger.Logger;

public class PerfKey implements Comparable<Object> {
	
	private static Logger logger = Logger.getLogger(PerfKey.class, IComponent.Id.JD.name());
	
	private String name;
	private String uniqueName;
	
	public PerfKey(String name, String uniqueName) {
		validate(name, uniqueName);
		setName(name);
		setUniqueName(uniqueName);
	}
	
	private void validate(String name, String uniqueName) {
		// NPE if missing
		name.length();
		uniqueName.length();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((uniqueName == null) ? 0 : uniqueName.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object o) {
		return compareTo(o) == 0;
	}
	
	@Override
	public int compareTo(Object o) {
		String location = "compareTo";
		int retVal = 0;
		if(o != null) {
			if(o instanceof PerfKey) {
				PerfKey that = (PerfKey) o;
				retVal = this.uniqueName.compareTo(that.uniqueName);
				if(retVal == 0) {
					int expect0 = this.name.compareTo(that.name);
					if(expect0 != 0) {
						MessageBuffer mb = new MessageBuffer();
						mb.append(Standardize.Label.name.get()+this.name);
						mb.append(Standardize.Label.name.get()+that.name);
						logger.warn(location, ILogger.null_id, mb.toString());
					}
				}
			}
		}
		return retVal;
	}
	
	private void setName(String value) {
		name = value;
	}
	
	public String getName() {
		return name;
	}
	
	private void setUniqueName(String value) {
		uniqueName = value;
	}
	
	public String getUniqueName() {
		return uniqueName;
	}
	
}
