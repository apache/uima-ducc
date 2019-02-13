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
package org.apache.uima.ducc.common.jd.files.perf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;

public class ViewJobPerformanceSummary {

  public static int cascount;
  /**
   * @param args
   */
  public static void main(String[] args) {
    if (args.length<1) {
      System.out.println("this command takes one arg: job-log-dir");
      System.exit(1);
    }
    PerformanceSummary performanceSummary = new PerformanceSummary(args[0]);
    PerformanceMetricsSummaryMap performanceMetricsSummaryMap = performanceSummary.readSummary();
    if (performanceMetricsSummaryMap == null || performanceMetricsSummaryMap.size() == 0) {
      System.err.println("Null map");
      System.exit(1);
    }
    cascount  = performanceMetricsSummaryMap.casCount();
    ArrayList <UimaStatistic> uimaStats = new ArrayList<UimaStatistic>();
    uimaStats.clear();
    long analysisTime = 0;
    try {
      for (Entry<String, PerformanceMetricsSummaryItem> entry : performanceMetricsSummaryMap.entrySet()) {
        String key = entry.getKey();
        int posName = key.lastIndexOf('=');
        long anTime = entry.getValue().getAnalysisTime();
        long anMinTime = entry.getValue().getAnalysisTimeMin();
        long anMaxTime = entry.getValue().getAnalysisTimeMax();
        long anTasks = entry.getValue().getAnalysisTasks();
        analysisTime += anTime;
        if (posName > 0) {
          String shortname = key.substring(posName+1);
          UimaStatistic stat = new UimaStatistic(shortname,
              entry.getKey(), anTime, anMinTime, anMaxTime, anTasks);
          uimaStats.add(stat);
        }
      }
      Collections.sort(uimaStats);
      int numstats = uimaStats.size();
      System.out.println("Job = "+args[0]);
      System.out.printf("Processed %d workitems, Average time = %.1f seconds%n", cascount, analysisTime/(1000.0*cascount));
      System.out.println("Component breakdown (ave time per workitem in sec):");
        for (int i = 0; i < numstats; ++i) {
          System.out.println(uimaStats.get(i).toString());
        }
    } catch (Exception e) {
      System.err.println("Problem parsing PerformanceMetricSummaryMap");
    }
  }

}
