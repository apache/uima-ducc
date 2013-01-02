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
package org.apache.uima.ducc.transport.event.common;

import java.util.Map;

import org.apache.uima.ducc.common.utils.DuccCollectionUtils;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils.DuccMapDifference;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils.DuccMapValueDifference;
import org.apache.uima.ducc.common.utils.id.DuccId;

import com.google.common.base.Preconditions;

public class DefaultJobManagerStateReconciler implements
		JobManagerStateReconciler {

	private WorkChangesHandler jobChangesCallback;
	private WorkProcessChangesHandler processChangesCallback;
	private DuccLogger logger;
	public DefaultJobManagerStateReconciler(DuccLogger logger) {
		this.logger = logger;
	}
	public void reconcile(Map<DuccId, IDuccWork> left, Map<DuccId, IDuccWork> right) {
		Preconditions.checkNotNull(jobChangesCallback, "JobChangesHandler Callback not Specified");
		Preconditions.checkNotNull(processChangesCallback, "JobProcessChangesHandler Callback not Specified");
		@SuppressWarnings("unchecked")
		DuccMapDifference<DuccId, IDuccWork> 
			jobDifferenceMap = DuccCollectionUtils.difference(left, right);
//		if ( jobDifferenceMap.getDifferingMap() != null && jobDifferenceMap.getDifferingMap().size() > 0 ) {
			try {
//				logger.info("DefaultJobManagerStateReconciler.reconcile", null, "Job Difference Map Size:"+jobDifferenceMap.getDifferingMap().size()+" New Orchestrator State:\n"+XStreamUtils.marshall(left));
				//	getLeft() returns Jobs that are in JobManager state Map only. These jobs will be added to the
				//  local state Map
				//logger.info("DefaultJobManagerStateReconciler.reconcile", null,"..........Calling onNewWork\n:"+XStreamUtils.marshall(jobDifferenceMap.getLeft()));
				jobChangesCallback.onNewWork(jobDifferenceMap.getLeft());
				//	getRight() returns Jobs that are in local state Map only. These jobs will be removed from the
				//	local state Map
				jobChangesCallback.onRemovedWork(jobDifferenceMap.getRight());

				//	Find differences between corresponding Jobs (existing in both Job Maps) 
				for( DuccMapValueDifference<IDuccWork> jd : jobDifferenceMap ) {
					//	getLeft() returns a Job with a different internal state from a Job from the getRight()
					jobChangesCallback.onWorkChanges(jd.getLeft(), jd.getRight());
				}

				
				// For jobs common to both Maps, iterate over diffs in respective Process Maps
				for( DuccMapValueDifference<IDuccWork> jd: jobDifferenceMap ) {
					// Job Process Maps dont match, find what Processes have added or deleted by diffing respective Process Maps
					@SuppressWarnings("unchecked")
					DuccMapDifference<DuccId, IDuccProcess> 
						processDifference = DuccCollectionUtils.
								difference(((IDuccWorkExecutable)jd.getLeft()).getProcessMap().getMap(), ((IDuccWorkExecutable)jd.getRight()).getProcessMap());
					// getLeft() returns Processes added 
					if ( processDifference.getLeft().size() > 0 ) {
//						processChangesCallback.onNewWorkProcesses(jd.getLeft().getDuccId(),((IDuccWorkExecutable)jd.getLeft()).getCommandLine(), processDifference.getLeft(),((IDuccWorkExecutable)jd.getRight()).getProcessMap().getMap());
						processChangesCallback.onNewWorkProcesses(jd.getLeft(),((IDuccWorkExecutable)jd.getLeft()).getCommandLine(), processDifference.getLeft(),((IDuccWorkExecutable)jd.getRight()).getProcessMap().getMap());
					}
					// getRight() returns Processes removed 
					if ( processDifference.getRight().size() > 0 ) {
						processChangesCallback.onRemovedWorkProcesses(jd.getLeft().getDuccId(), processDifference.getRight(),((IDuccWorkExecutable)jd.getRight()).getProcessMap().getMap());
					}
					// For Processes common to both Process Maps, handle Process state changes 
					for( DuccMapValueDifference<IDuccProcess> pd : processDifference ) {
						processChangesCallback.onProcessChanges(jd.getLeft(),pd.getLeft(), pd.getRight());
					}
				}
			} catch( Exception e) {
				logger.error("DefaultJobManagerStateReconciler.reconcile", null, e);
			}
		//}
	}

	public void setWorkChangesHandler(WorkChangesHandler callback) {
		this.jobChangesCallback = callback;
	}

	public void setWorkProcessChanges(WorkProcessChangesHandler callback) {
		this.processChangesCallback = callback;
	}
	
}
