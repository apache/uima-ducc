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

package org.apache.uima.ducc.ws.cli;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.List;

import org.apache.uima.ducc.ws.cli.json.NodePidList;
import org.apache.uima.ducc.ws.cli.json.ReservationFacts;
import org.apache.uima.ducc.ws.cli.json.ReservationFactsList;

import com.google.gson.Gson;

public class DuccWebQueryReservations extends DuccWebQuery {

	private DuccWebQueryReservations() {
		super("/ducc-servlet/json-format-reservations");
	}

	private ReservationFactsList get() throws Exception {
		URL url = new URL(getUrlString());
		URLConnection ucon = url.openConnection();
		BufferedReader br = new BufferedReader(new InputStreamReader(ucon.getInputStream()));
		String jSon = br.readLine();
		Gson gson = new Gson();
		ReservationFactsList fl = gson.fromJson(jSon, ReservationFactsList.class);
        br.close();
        return fl;
	}
	
	private String stringify(List<NodePidList> list) {
		StringBuffer sb = new StringBuffer();
		for(NodePidList nodePids : list) {
			String node = nodePids.node;
			StringBuffer pb = new StringBuffer();
			for(String pid : nodePids.pids) {
				if(pb.length() > 0) {
					pb.append(",");
				}
				pb.append(pid);
			}
			String pids = pb.toString();
			if(sb.length() > 0) {
				sb.append(",");
			}
			sb.append(node+"{"+pids+"}");
		}
		return "["+sb.toString()+"]";
	}
	
	private void display(ReservationFactsList fl) {
		if(fl != null) {
			Iterator<ReservationFacts> fIterator = fl.iterator();
			while(fIterator.hasNext()) {
				ReservationFacts f = fIterator.next();
				System.out.println(f.id);
				System.out.println("  "+"start: "+f.start);
				System.out.println("  "+"end: "+ f.end);
				System.out.println("  "+"user: "+ f.user);
				System.out.println("  "+"clas: "+ f.rclass);
				System.out.println("  "+"state: "+ f.state);
				System.out.println("  "+"reason: "+ f.reason);
				System.out.println("  "+"allocation: "+ f.allocation);
				System.out.println("  "+"processes: "+ stringify(f.userProcesses));
				System.out.println("  "+"size: "+ f.size);
				System.out.println("  "+"list: "+ f.list);
				System.out.println("  "+"description: "+ f.description);
			}
		}
		else {
			System.out.println("?");
		}
	}
	
	private void main_instance(String[] args) throws Exception {
		ReservationFactsList fl = get();
		display(fl);
	}
	
	// TODO: Add support for maxRecords=<n> query parameter
	
	public static void main(String[] args) {
		try {
			DuccWebQueryReservations dwq = new DuccWebQueryReservations();
			dwq.main_instance(args);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}