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
package org.apache.uima.ducc.agent.metrics.collectors;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.concurrent.Callable;

import org.apache.uima.ducc.common.node.metrics.NodeLoadAverage;
import org.apache.uima.ducc.common.node.metrics.UptimeNodeLoadAverage;


public class NodeLoadAverageCollector extends AbstractMetricCollector 
implements Callable<NodeLoadAverage>{
  
 public NodeLoadAverageCollector() {
	super(null,0,0);
	
 }
  public NodeLoadAverageCollector(RandomAccessFile metricFile,  int howMany, int offset) {
    super(metricFile, howMany, offset);
  }

  public NodeLoadAverage call() throws Exception {
    //super.parseMetricFile();
    
//	 UptimeNodeLoadAverage uptimeLoadAverage = new UptimeNodeLoadAverage();
	 return collect();
	 //return new NodeLoadAverageInfo(super.metricFileContents, super.metricFieldOffsets, super.metricFieldLengths);
  }
	private NodeLoadAverage collect() throws Exception {
		   InputStream stream = null;
		   BufferedReader reader = null;
		   UptimeNodeLoadAverage uptimeLoadAverage = new UptimeNodeLoadAverage(); 
		   ProcessBuilder pb = 
				   new ProcessBuilder("uptime");;
	       pb.redirectErrorStream(true);
		   Process proc = pb.start();
		   //  spawn uptime command and scrape the output
		   stream = proc.getInputStream();
		   reader = new BufferedReader(new InputStreamReader(stream));
		   String line;
		   String regex = "\\s+";
		   String filter = "load average:";
		   // read the next line from ps output
		   while ((line = reader.readLine()) != null) {
//			   System.out.println("UPTIME:"+line);
		       int pos=0;   
			   if ( (pos = line.indexOf(filter)) > -1 ) {
		          String la =  line.substring(pos+filter.length()).replaceAll(regex,"");
				  String[] averages = la.split(",");
				  uptimeLoadAverage.setLoadAvg1(averages[0]);
				  uptimeLoadAverage.setLoadAvg1(averages[1]);
				  uptimeLoadAverage.setLoadAvg1(averages[2]);
			   }
		   }
		   proc.waitFor();
		   return uptimeLoadAverage; 
		}
}
