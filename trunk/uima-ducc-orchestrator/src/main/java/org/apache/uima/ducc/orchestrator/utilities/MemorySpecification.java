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
package org.apache.uima.ducc.orchestrator.utilities;

import org.apache.uima.ducc.transport.event.common.IDuccUnits;

public class MemorySpecification {

	private String msize = null;
	private String units = null;
	private IDuccUnits.MemoryUnits memUnits = null;
	
	public MemorySpecification(String memorySpecification) {
		init(memorySpecification);
	}
	
	private void init(String memorySpecification) {
		if(memorySpecification != null) {
			msize = memorySpecification.replaceAll("\\s","");
			if(msize.endsWith("KB")) {
				units = "KB";
				memUnits = IDuccUnits.MemoryUnits.KB;
				msize = msize.substring(0,msize.length()-2);
			}
			else if(msize.endsWith("MB")) {
				units = "MB";
				memUnits = IDuccUnits.MemoryUnits.MB;
				msize = msize.substring(0,msize.length()-2);
			}
			else if(msize.endsWith("GB")) {
				units = "GB";
				memUnits = IDuccUnits.MemoryUnits.GB;
				msize = msize.substring(0,msize.length()-2);
			}
			else if(msize.endsWith("TB")) {
				units = "TB";
				memUnits = IDuccUnits.MemoryUnits.TB;
				msize = msize.substring(0,msize.length()-2);
			}
			try {
				Integer.parseInt(msize);
			}
			catch(Exception e) {
				msize = null;
			}
		}
	}
	
	public String getSize() {
		return msize;
	}
	
	public String getUnits() {
		return units;
	}
	
	public IDuccUnits.MemoryUnits getMemoryUnits() {
		return memUnits;
	}
}
