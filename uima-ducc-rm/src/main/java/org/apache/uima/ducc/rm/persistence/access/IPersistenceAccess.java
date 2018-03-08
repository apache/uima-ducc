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
package org.apache.uima.ducc.rm.persistence.access;

import java.util.Map;

import org.apache.uima.ducc.common.persistence.rm.IDbJob;
import org.apache.uima.ducc.common.persistence.rm.IDbShare;
import org.apache.uima.ducc.common.persistence.rm.IRmPersistence.RmNodes;
import org.apache.uima.ducc.common.utils.id.DuccId;

public interface IPersistenceAccess {

	public void setNodeProperty(String id, RmNodes key, Object value) throws Exception;
	public void setNodeProperties(String id, Object... properties) throws Exception;
	public void addAssignment(String id, DuccId jobid, IDbShare share, int quantum, String jobtype) throws Exception;
	public void removeAssignment(String id, DuccId jobid, IDbShare share) throws Exception;
	public void createMachine(String id, Map<RmNodes, Object> props) throws Exception;
	public void clear() throws Exception;
	public void close();
	public void addJob(IDbJob j) throws Exception;
    public void deleteJob(IDbJob j) throws Exception;
    public void updateDemand(IDbJob j) throws Exception;
    public void updateShare(String node, DuccId shareid, DuccId jobid, long investment, String state, long init_time, long pid) throws Exception;
    public void setFixed(String node, DuccId shareId, DuccId jobId, boolean val) throws Exception;
    public void setPurged(String node, DuccId shareId, DuccId jobId, boolean val) throws Exception;
    public void setEvicted(String node, DuccId shareId, DuccId jobId, boolean val) throws Exception;

}
