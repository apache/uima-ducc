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
package org.apache.uima.ducc.orchestrator;

import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.id.IDuccIdFactory;
import org.apache.uima.ducc.orchestrator.utilities.MemorySpecification;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;
import org.apache.uima.ducc.transport.event.common.DuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.DuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;


public class ReservationFactory {
	private static ReservationFactory reservationFactory = new ReservationFactory();
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(ReservationFactory.class.getName());
	
	public static ReservationFactory getInstance() {
		return reservationFactory;
	}
	
	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private Messages messages = orchestratorCommonArea.getSystemMessages();
	private IDuccIdFactory duccIdFactory = orchestratorCommonArea.getDuccIdFactory();
	
	public DuccWorkReservation create(CommonConfiguration common, ReservationRequestProperties reservationRequestProperties) {
		String methodName = "create";
		logger.trace(methodName, null, messages.fetch("enter"));
		DuccWorkReservation duccWorkReservation = new DuccWorkReservation();
		// id, type
		duccWorkReservation.setDuccId(duccIdFactory.next());
		duccWorkReservation.setDuccType(DuccType.Reservation);
		// standard info
		DuccStandardInfo standardInfo = new DuccStandardInfo();
		duccWorkReservation.setStandardInfo(standardInfo);
		standardInfo.setUser(reservationRequestProperties.getProperty(ReservationSpecificationProperties.key_user));
		standardInfo.setSubmitter(reservationRequestProperties.getProperty(ReservationSpecificationProperties.key_submitter_pid_at_host));
		standardInfo.setDateOfSubmission(TimeStamp.getCurrentMillis());
		standardInfo.setDateOfCompletion(null);
		standardInfo.setDescription(reservationRequestProperties.getProperty(ReservationSpecificationProperties.key_description));
		// scheduling info
		DuccSchedulingInfo schedulingInfo = new DuccSchedulingInfo();
		duccWorkReservation.setSchedulingInfo(schedulingInfo);
		schedulingInfo.setSchedulingClass(reservationRequestProperties.getProperty(ReservationSpecificationProperties.key_scheduling_class));
		String memorySize = reservationRequestProperties.getProperty(ReservationSpecificationProperties.key_memory_size);
		MemorySpecification memorySpecification = new MemorySpecification(memorySize);
		schedulingInfo.setShareMemorySize(memorySpecification.getSize());
		schedulingInfo.setShareMemoryUnits(memorySpecification.getMemoryUnits());
		logger.info(methodName, duccWorkReservation.getDuccId(), messages.fetchLabel("user")+standardInfo.getUser());
		logger.info(methodName, duccWorkReservation.getDuccId(), messages.fetchLabel("description")+standardInfo.getDescription());
		logger.info(methodName, duccWorkReservation.getDuccId(), messages.fetchLabel("class")+schedulingInfo.getSchedulingClass());
		logger.info(methodName, duccWorkReservation.getDuccId(), messages.fetchLabel("priority")+schedulingInfo.getSchedulingPriority());
		logger.info(methodName, duccWorkReservation.getDuccId(), messages.fetchLabel("memory")+schedulingInfo.getShareMemorySize()+schedulingInfo.getShareMemoryUnits());
		logger.info(methodName, duccWorkReservation.getDuccId(), messages.fetchLabel("instances")+schedulingInfo.getInstancesCount());
		logger.trace(methodName, null, messages.fetch("exit"));
		return duccWorkReservation;
	}
}
