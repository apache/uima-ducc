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

public class PerfKey implements Comparable<Object> {

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
	public int compareTo(Object o) {
		int retVal = 0;
		if(o != null) {
			if(o instanceof PerfKey) {
				PerfKey that = (PerfKey) o;
				retVal = this.name.compareTo(that.name);
				if(retVal == 0) {
					retVal = this.uniqueName.compareTo(that.uniqueName);
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
