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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;


public interface IHistoryPersistenceManager {

	public void jobSaveConditional(IDuccWorkJob duccWorkJob) throws IOException;
	public void jobSave(IDuccWorkJob duccWorkJob) throws IOException;
	public IDuccWorkJob jobRestore(String fileName);
	public IDuccWorkJob jobRestore(DuccId duccId);
	public ArrayList<String> jobList();
	public ArrayList<IDuccWorkJob> jobRestore() throws IOException, ClassNotFoundException;
	
	public void reservationSaveConditional(IDuccWorkReservation duccWorkReservation) throws IOException;
	public void reservationSave(IDuccWorkReservation duccWorkReservation) throws IOException;
	public IDuccWorkReservation reservationRestore(String fileName);
	public IDuccWorkReservation reservationRestore(DuccId duccId);
	public ArrayList<String> reservationList();
	public ArrayList<IDuccWorkReservation> reservationRestore() throws IOException, ClassNotFoundException;
	
	public void serviceSaveConditional(IDuccWorkService duccWorkService) throws IOException;
	public void serviceSave(IDuccWorkService duccWorkService) throws IOException;
	public IDuccWorkService serviceRestore(String fileName);
	public IDuccWorkService serviceRestore(DuccId duccId);
	public ArrayList<String> serviceList();
	public ArrayList<IDuccWorkService> serviceRestore() throws IOException, ClassNotFoundException;
}
