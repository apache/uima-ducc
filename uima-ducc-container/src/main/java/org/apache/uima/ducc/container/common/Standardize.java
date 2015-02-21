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
package org.apache.uima.ducc.container.common;

public class Standardize {

	public enum Label {
		text,
		limit,
		classname,
		exception,
		instance,
		id,
		skip,
		dispatched,
		deadline,
		sum,
		count,
		total,
		name,
		key,
		value,
		size,
		status,
		add,
		puts,
		gets,
		reason,
		enter,
		exit,
		dispatch,
		done,
		error,
		preempt,
		retry,
		avg,
		min,
		max,
		removed,
		isKillJob,
		isKillProcess,
		isKillWorkItem,
		current,
		request,
		result,
		remainder,
		jdObject,
		jdState,
		directory,
		node,
		ip,
		pidName,
		pid,
		tid,
		crFetches,
		crTotal,
		retrys,
		preemptions,
		endSuccess,
		endFailure,
		finishedMillisMax,
		finishedMillisMin,
		finishedMillisAvg,
		runningMillisMax,
		runningMillisMin,
		todMostRecentStart,
		state,
		event,
		curr,
		prev,
		hash,
		loaded,
		loading,
		seqNo,
		transNo,
		remote,
		action,
		type,
		AckMsecs,
		EndMsecs,
		killJob,
		killProcess,
		killWorkItem,
		operatingMillis,
		;
		
		Label() {
		}
		
		public String get() {
			return this+"=";
		}
	}
}
