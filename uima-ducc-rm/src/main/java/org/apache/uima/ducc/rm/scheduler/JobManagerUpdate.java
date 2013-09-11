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
package org.apache.uima.ducc.rm.scheduler;

import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.common.utils.id.DuccId;



/**
 * This is what gets sent to job manager when there are scheduling decisions to act upon.
 */
public class JobManagerUpdate
{

    private Map<DuccId, HashMap<Share, Share>> expandedShares = new HashMap<DuccId, HashMap<Share, Share>>();  // by job, which shares to add

    private Map<DuccId, HashMap<Share, Share>> shrunkenShares = new HashMap<DuccId, HashMap<Share, Share>>();  // by job, which shares to remove

    private Map<DuccId, IRmJob> allJobs;                   // current state of all jobs

    @SuppressWarnings("unchecked")
	public void setAllJobs(HashMap<DuccId, IRmJob> jobs)
    {
        allJobs = (HashMap<DuccId, IRmJob>) jobs.clone();                            // shallow copy intentional
    }

    public Map<DuccId, IRmJob> getAllJobs()
    {
        return allJobs;
    }

    public void addRefusal(IRmJob job)
    {
        allJobs.put(job.getId(), job);
    }

    @SuppressWarnings("unchecked")
    public void addShares(IRmJob job, HashMap<Share, Share> shares)
    {
        expandedShares.put(job.getId(), (HashMap<Share, Share>) shares.clone());                             // shallow copy intentional
    }
    
	public Map<DuccId, HashMap<Share, Share>> getExpandedShares() {
		return expandedShares;
	}

	public Map<DuccId, HashMap<Share, Share>> getShrunkenShares() {
		return shrunkenShares;
	}

    @SuppressWarnings("unchecked")
	public void removeShares(IRmJob job, HashMap<Share, Share> shares) 
    {
        shrunkenShares.put(job.getId(), (HashMap<Share, Share>) shares.clone());    // shallow copy is intentional
    }
}
