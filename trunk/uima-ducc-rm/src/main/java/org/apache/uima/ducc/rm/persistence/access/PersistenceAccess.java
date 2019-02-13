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

import org.apache.uima.ducc.common.head.IDuccHead;
import org.apache.uima.ducc.common.persistence.rm.IDbJob;
import org.apache.uima.ducc.common.persistence.rm.IDbShare;
import org.apache.uima.ducc.common.persistence.rm.IRmPersistence;
import org.apache.uima.ducc.common.persistence.rm.IRmPersistence.RmNodes;
import org.apache.uima.ducc.common.persistence.rm.RmPersistenceFactory;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.rm.DuccHead;
import org.apache.uima.ducc.rm.scheduler.SchedConstants;

public class PersistenceAccess implements IPersistenceAccess {

	private static String COMPONENT_NAME = SchedConstants.COMPONENT_NAME;
	private static DuccId jobid = null;
	private static DuccLogger logger = DuccLogger.getLogger(PersistenceAccess.class, COMPONENT_NAME);
	
	private static IPersistenceAccess persistenceAccess = new PersistenceAccess();
	
	private static String bypass = "bypass";
	
	private IDuccHead dh = DuccHead.getInstance();
	private IRmPersistence rm_persistence = null;

	public static IPersistenceAccess getInstance() {
		return persistenceAccess;
	}
	
	private PersistenceAccess() {
		rm_persistence = RmPersistenceFactory.getInstance(this.getClass().getName(), COMPONENT_NAME);
	}

	private boolean is_master() {
		String location = "is_master";
		boolean retVal = dh.is_ducc_head_virtual_master();
		logger.debug(location, jobid, retVal);
		return retVal;
	}
	
	@Override
	public void setNodeProperty(String id, RmNodes key, Object value) throws Exception {
		String location = "setNodeProperty";
		if(is_master()) {
			rm_persistence.setNodeProperty(id, key, value);
		}
		else {
			logger.debug(location, jobid, bypass, id, key, value);
		}
	}

	@Override
	public void setNodeProperties(String id, Object... properties) throws Exception {
		String location = "setNodeProperties";
		if(is_master()) {
			rm_persistence.setNodeProperties(id, properties);
		}
		else {
			logger.debug(location, jobid, bypass, id);
		}
	}

	@Override
	public void addAssignment(String id, DuccId jobid, IDbShare share, int quantum, String jobtype) throws Exception {
		String location = "addAssignment";
		if(is_master()) {
			rm_persistence.addAssignment(id, jobid, share, quantum, jobtype);
		}
		else {
			logger.debug(location, jobid, bypass, id, jobid, share, quantum, jobtype);
		}
	}

	@Override
	public void removeAssignment(String id, DuccId jobid, IDbShare share) throws Exception {
		String location = "removedAssignment";
		if(is_master()) {
			rm_persistence.removeAssignment(id, jobid, share);
		}
		else {
			logger.debug(location, jobid, bypass, id, jobid, share);
		}
	}

	@Override
	public void createMachine(String id, Map<RmNodes, Object> props) throws Exception {
		String location = "createMachine";
		if(is_master()) {
			rm_persistence.createMachine(id, props);
		}
		else {
			logger.debug(location, jobid, bypass, id);
		}
	}

	@Override
	public void clear() throws Exception {
		String location = "clear";
		if(is_master()) {
			rm_persistence.clear();
		}
		else {
			logger.debug(location, jobid, bypass);
		}
	}

	@Override
	public void close() {
		String location = "close";
		if(is_master()) {
			rm_persistence.close();
		}
		else {
			logger.debug(location, jobid, bypass);
		}
	}

	@Override
	public void addJob(IDbJob j) throws Exception {
		String location = "addJob";
		if(is_master()) {
			rm_persistence.addJob(j);
		}
		else {
			logger.debug(location, jobid, bypass, j);
		}
	}

	@Override
	public void deleteJob(IDbJob j) throws Exception {
		String location = "deleteJob";
		if(is_master()) {
			rm_persistence.deleteJob(j);
		}
		else {
			logger.debug(location, jobid, bypass, j);
		}
	}

	@Override
	public void updateDemand(IDbJob j) throws Exception {
		String location = "updateDemand";
		if(is_master()) {
			rm_persistence.updateDemand(j);
		}
		else {
			logger.debug(location, jobid, bypass, j);
		}
	}

	@Override
	public void updateShare(String node, DuccId shareid, DuccId jobid, long investment, String state, long init_time, long pid) throws Exception {
		String location = "updateShare";
		if(is_master()) {
			rm_persistence.updateShare(node, shareid, jobid, investment, state, init_time, pid);
		}
		else {
			logger.debug(location, jobid, bypass, node, shareid, jobid, investment, state, init_time, pid);
		}
	}

	@Override
	public void setFixed(String node, DuccId shareId, DuccId jobId, boolean val) throws Exception {
		String location = "setFixed";
		if(is_master()) {
			rm_persistence.setFixed(node, shareId, jobId, val);
		}
		else {
			logger.debug(location, jobId, bypass, node, shareId, jobId, val);
		}
	}

	@Override
	public void setPurged(String node, DuccId shareId, DuccId jobId, boolean val) throws Exception {
		String location = "setPurged";
		if(is_master()) {
			rm_persistence.setPurged(node, shareId, jobId, val);
		}
		else {
			logger.debug(location, jobId, bypass, node, shareId, jobId, val );
		}
	}

	@Override
	public void setEvicted(String node, DuccId shareId, DuccId jobId, boolean val) throws Exception {
		String location = "setEvicted";
		if(is_master()) {
			rm_persistence.setEvicted(node, shareId, jobId, val);
		}
		else {
			logger.debug(location, jobId, bypass, node, shareId, jobId, val );
		}
	}
	
}
