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
package org.apache.uima.ducc.transport.event.common.history;

import java.util.List;
import java.util.Map;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;


public interface IHistoryPersistenceManager 
{
	public void                       saveJob(IDuccWorkJob duccWorkJob) throws Exception;
	public IDuccWorkJob               restoreJob(long friendly_id)      throws Exception;
	public List<IDuccWorkJob>         restoreJobs(long max)             throws Exception;
	
	public void                       saveReservation(IDuccWorkReservation reservation) throws Exception;
	public IDuccWorkReservation       restoreReservation(long friendly_id)              throws Exception;
	public List<IDuccWorkReservation> restoreReservations(long max)                     throws Exception;

	public void                       saveService(IDuccWorkService duccWorkService) throws Exception;
	public IDuccWorkService           restoreService(long friendly_id)              throws Exception;
	public List<IDuccWorkService>     restoreServices(long max)                     throws Exception;

    public boolean checkpoint(DuccWorkMap work, Map<DuccId, DuccId> processToJob)   throws Exception;
    public boolean restore(DuccWorkMap work, Map<DuccId, DuccId> processToJob)      throws Exception;

    public void setLogger(DuccLogger logger);
	//public void serviceSaveConditional(IDuccWorkService duccWorkService) throws Exception;
	// public void serviceSave(IDuccWorkService duccWorkService) throws Exception;
	//public IDuccWorkService serviceRestore(String fileName);
	//public IDuccWorkService serviceRestore(DuccId duccId);
	//public ArrayList<String> serviceList();
	//public ArrayList<IDuccWorkService> serviceRestore() throws IOException, ClassNotFoundException;
}
