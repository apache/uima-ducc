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
package org.apache.uima.ducc.ws;

import org.apache.uima.ducc.cli.ws.json.MachineFacts;
import org.apache.uima.ducc.cli.ws.json.MachineFactsList;

public class DuccMachinesDataHelper {
	
	/**
	 * @param factsList = list of machine facts, each entry comprising one machine
	 * @param name = the machine name of the entry to be located
	 * @return = the entry comprising the located machine
	 */
	public static MachineFacts getMachineFacts(MachineFactsList factsList, String name) {
		MachineFacts retVal = null;
		if(factsList != null) {
			if(name != null) {
				for(MachineFacts facts : factsList) {
					if(facts.name != null) {
						if(name.equals(facts.name)) {
							retVal = facts;
							break;
						}
					}
					
				}
			}
		}
		return retVal;
	}
	
	/**
	 * @param facts = facts of a machine
	 * @return true if "up" false otherwise
	 */
	public static boolean isUp(MachineFacts facts) {
		boolean retVal = false;
		if(facts != null) {
			if(facts.status.equals("up")) {
				retVal = true;
			}
		}
		return retVal;
	}

}
