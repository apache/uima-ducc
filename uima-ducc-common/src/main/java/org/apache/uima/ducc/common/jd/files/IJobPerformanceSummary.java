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
package org.apache.uima.ducc.common.jd.files;

public interface IJobPerformanceSummary {
	
	public String getName();
	public void setName(String value);
	
	public String getUniqueName();
	public void setUniqueName(String value);
	
	public long getAnalysisTime();
	public void setAnalysisTime(long value);
	
	public long getAnalysisTimeMin();
	public void setAnalysisTimeMin(long value);
	
	public long getAnalysisTimeMax();
	public void setAnalysisTimeMax(long value);
	
	public long getAnalysisTasks();
	public void setAnalysisTasks(long value);
	
	public long getNumProcessed();
	public void setNumProcessed(long value);
}
