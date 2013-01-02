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

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.event.common.IDuccWork;


/**
 * Reconciles two given Maps containing Job Manager's state. The left Map 
 * is what Job Manager posts at regular intervals. It contains the most up to date 
 * information about Jobs and Processes. The right Map is the local Map that a 
 * component maintains. It reflects the state of the Job Manager and it should stay in 
 * sync with the left Map. The reconciliation is done across Jobs as well as Process 
 * Maps. The reconciliation detects:
 * <ul>
 *   <li>Jobs added</li>
 *   <li>Jobs removed</li>
 *   <li>Processes added to Jobs</li>
 *   <li>Processes removed from Jobs</li>
 *   <li>Processes in both Job Process Maps that are different (by state, PID)</li> 
 * </ul>
 * <p> 
 * This class uses registered callback handlers to notify an application of events 
 * related to what the reconciler detects. An application must register the following 
 * callback handlers:
 * <ul>
 *   <li><@link JobChangesHandler></li>
 *   <li><@link JobProcessChangesHandler)></li>
 * </ul>
 * The <@code reconcile()> method will fail if callback handlers are not injected
 * prior to calling it. 
 * <p> 
 * The reconciliation is performed in multiple stages. First, new Jobs are detected and
 * added to an intermediate Map which is than passed as an argument to   
 * {@code JobChangesHandler.onNewJobs()} handler. Next, removed Jobs are detected and
 * added to an intermediate Map which is than passed as an argument to
 * {@code JobChangesHandler.onRemovedJobs()} handler. Next, on per Job basis, new
 * Processes are detected and added to an intermediate Map which is than passed as 
 * an argument to {@code JobProcessChangesHandler.onNewJobProcesses()} handler. Following,
 * that, on per Job basis, removed Processes are detected and added to an intermediate
 * Map which is than passed as an argument to
 * {@code JobProcessChangesHandler.onRemovedJobProcesses()} handler. Finally, for
 * each Process that exists in both Process Maps, changes are detected and those 
 * Processes are passed as arguments to {@code JobProcessChangesHandler.onProcessChanges()}
 * handler.
 *
 */
public interface JobManagerStateReconciler {
	/**
	 * Main method the reconciles Job Manager's state. 
	 * 
	 * @param left - most current state Map sent by Job Manager 
	 * @param right - current Job Manager state Map maintained by a component 
	 */
	public void reconcile(Map<DuccId, IDuccWork> left, Map<DuccId, IDuccWork> right);
	
	/**
	 * Injects callback listener to receive Job changes
	 * 
	 * @param callback - callback to notify of job changes
	 */
	public void setWorkChangesHandler(WorkChangesHandler callback);
	/**
	 * Injects callback listener to receive Job process changes
	 * 
	 * @param callback - callback to notify of process changes
	 */
	public void setWorkProcessChanges(WorkProcessChangesHandler callback);

	/**
	 * Callback listener to receive notifications when job changes are detected
	 * during reconciliation. An application *must* inject instance of this 
	 * callback listener *before* calling reconcile() method.
	 *
	 */
	public interface WorkChangesHandler {
		/**
		 * Called when new Jobs are detected during reconciliation. This method 
		 * is called once, when the reconciliation finishes.  
		 * 
		 * @param newJobsMap - map containing new jobs
		 */
		public void onNewWork(Map<DuccId, IDuccWork> newWorkMap);
		/**
		 * Called when removed Jobs are detected during reconciliation. This method 
		 * is called once, when the reconciliation finishes.    
		 * 
		 * @param removedJobsMap - map containing removed jobs
		 */
		public void onRemovedWork(Map<DuccId, IDuccWork> removedWorkMap);
		
		/**
		 * Called when a Job in both Job Maps has a different internal state. That
		 * can be due to status change, etc. This method is called once per each
		 * Job that has a different state in both Maps. 
		 * 
		 * @param left - Job with a new internal state
		 * @param right - local Job which must be sync'ed with left
		 */
		public void onWorkChanges(IDuccWork left, IDuccWork right);
	}
	/**
	 * Callback listener to receive notifications when job's process changes are 
	 * detected during reconciliation. An application *must* inject instance of this 
	 * callback listener *before* calling reconcile() method.
	 *
	 */
	public interface WorkProcessChangesHandler {
		/**
		 *  Called when new processes are added to existing Jobs. This method 
		 *  can be called multiple times. It is called for each Job whose process(es)
		 *  were added.   
		 *   
		 * @param newJobProcessMap - Map containing new processes
		 * @param newJobProcessMapToUpdate - local Process Map to update
		 */
		public void onNewWorkProcesses(IDuccWork work, ICommandLine commandLine, Map<DuccId, IDuccProcess> newWorkProcessMap, Map<DuccId, IDuccProcess> newWorkProcessMapToUpdate);
		/**
		 * Called when removed processes are detected. This method can be called 
		 * multiple times. It is called for each Job whose process(es) where removed.
		 * 
		 * @param removedJobProcessMap - Map containing removed processes 
		 * @param newJobProcessMapToUpdate - local Process Map to update
		 */
		public void onRemovedWorkProcesses(DuccId jobId, Map<DuccId, IDuccProcess> removedWorkProcessMap, Map<DuccId, IDuccProcess> newWorkProcessMapToUpdate);
		/**
		 * Called when a Process in both Process Maps has a different internal state. That
		 * can be due to assigned PID, status, etc. This method is called once per each
		 * process that has a different state in both Maps. 
		 * 
		 * @param left - Process with a new internal state
		 * @param right - local Process which must be sync'ed with left
		 */
		public void onProcessChanges(IDuccWork job, IDuccProcess left, IDuccProcess right);
	}
}
