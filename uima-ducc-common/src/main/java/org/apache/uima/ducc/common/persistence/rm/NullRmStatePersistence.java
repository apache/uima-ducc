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
package org.apache.uima.ducc.common.persistence.rm;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class NullRmStatePersistence implements IRmPersistence
{
				
	NullRmStatePersistence() {
	}
	
    public void init(DuccLogger logger) throws Exception { }
    public void clear() {}
    public void close() {}
    public void setNodeProperty(String id, RmNodes key, Object value) { }
    public void setNodeProperties(String id, Object... props) {}
    public void createMachine(String id, Map<RmNodes, Object> props) { }
    public void addAssignment(String id, DuccId jobid, IDbShare shareid, int quantum, String type) {}
    public void removeAssignment(String id, DuccId jobid, IDbShare shareid) {}
    public void setEvicted(String node, DuccId shareId, DuccId jobId, boolean val) {}
    public void setFixed(String node, DuccId shareId, DuccId jobId, boolean val) {}
    public void setPurged(String node, DuccId shareId, DuccId jobId, boolean val) {}
    public void updateShare(String node, DuccId shareid, DuccId jobid, long investment, String state, long init_time, long pid) {}
    public Properties getMachine(String id) { return null; }
    public Map<String, Map<String, Object>> getAllMachines() { return new HashMap<String, Map<String, Object>>(); }
}
