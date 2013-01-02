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
package org.apache.uima.ducc.transport.event.jd;

public class UimaStatistic implements Comparable<UimaStatistic> {

	  private String shortName;
	  private long analysisTime;
	  private long analysisMinTime;
	  private long analysisMaxTime;
	  private String longName;

	  public UimaStatistic (String shortName, String longName, long analysisTime, long anMinTime, long anMaxTime) {
	    this.shortName = shortName;
	    this.analysisTime = analysisTime;
	    this.longName = longName;
	    this.analysisMinTime = anMinTime;
	    this.analysisMaxTime = anMaxTime;
	  }

	  @Override
	  public int compareTo(UimaStatistic other) {
	    return - Long.signum(analysisTime - other.analysisTime);
	  }

	  @Override
	  public String toString() {
//	    return "UimaStatistic [name=" + shortName + ", analysisTime=" + analysisTime
//	            + ", longName=" + longName + "]";
	    return String.format("   %s: %.2f",shortName, analysisTime/(1000.0*ViewJobPerformanceSummary.cascount));
	  }

	  public String getShortName() {
	    return shortName;
	  }

	  public long getAnalysisTime() {
	    return analysisTime;
	  }

	  public long getAnalysisMinTime() {
	    return analysisMinTime;
	  }

	  public long getAnalysisMaxTime() {
	    return analysisMaxTime;
	  }

	  public String getLongName() {
	    return longName;
	  }

	  public String getToolTip() {
	    return shortName + " ("+ longName + ")";
	  }
}
