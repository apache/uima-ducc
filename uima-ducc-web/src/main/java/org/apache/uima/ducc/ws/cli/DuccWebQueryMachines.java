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

import org.apache.uima.ducc.ws.cli.json.MachineFacts;
import org.apache.uima.ducc.ws.cli.json.MachineFactsList;

import com.google.gson.Gson;

public class DuccWebQueryMachines extends DuccWebQuery {

	private DuccWebQueryMachines() {
		super("/ducc-servlet/json-format-machines");
	}

	private MachineFactsList get() throws Exception {
		URL url = new URL(getUrlString());
		URLConnection ucon = url.openConnection();
		BufferedReader br = new BufferedReader(new InputStreamReader(ucon.getInputStream()));
		String jSon = br.readLine();
		Gson gson = new Gson();
		MachineFactsList fl = gson.fromJson(jSon, MachineFactsList.class);
        br.close();
        return fl;
	}
	
	private String stringify(List<String> list) {
		StringBuffer sb = new StringBuffer();
		for(String item : list) {
			if(sb.length() > 0) {
				sb.append(",");
			}
			sb.append(item);
		}
		return "["+sb.toString()+"]";
	}
	
	private void display(MachineFactsList fl) {
		if(fl != null) {
			Iterator<MachineFacts> fIterator = fl.iterator();
			while(fIterator.hasNext()) {
				MachineFacts f = fIterator.next();
				System.out.println(f.name);
				System.out.println("  "+"status: "+f.status);
				System.out.println("  "+"aliens: "+ stringify(f.aliens));
				System.out.println("  "+"swapInuse: "+ f.swapInuse);
				System.out.println("  "+"swapFree: "+ f.swapFree);
				System.out.println("  "+"memoryTotal: "+ f.memTotal);
				System.out.println("  "+"memoryFree: "+ f.memFree);
				System.out.println("  "+"ip: "+ f.ip);
				System.out.println("  "+"heartbeat: "+ f.heartbeat);
			}
		}
		else {
			System.out.println("?");
		}
	}
	
	private void main_instance(String[] args) throws Exception {
		MachineFactsList fl = get();
		display(fl);
	}
	
	public static void main(String[] args) {
		try {
			DuccWebQueryMachines dwq = new DuccWebQueryMachines();
			dwq.main_instance(args);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}