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
package org.apache.uima.ducc.common.node.metrics;


public class UptimeNodeLoadAverage implements NodeLoadAverage {


	private static final long serialVersionUID = 1L;
	String la1 = "N/A";
	String la5 = "N/A";
	String la15 = "N/A";
	public String getLoadAvg1() {
		return la1;
	}
	public void setLoadAvg1(String val) {
		la1 = val;
	}
	public void setLoadAvg5(String val) {
		la5 = val;
	}
	public void setLoadAvg15(String val) {
		la15 = val;
	}
	public String getLoadAvg5() {
		return la5;
	}

	public String getLoadAvg15() {
		return la15;
	}

	@Override
	public String getCurrentRunnableProcessCount() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTotalProcessCount() {
		// TODO Auto-generated method stub
		return null;
	}

}
