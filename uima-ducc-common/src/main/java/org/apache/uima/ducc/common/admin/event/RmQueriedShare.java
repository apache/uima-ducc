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
package org.apache.uima.ducc.common.admin.event;

import java.io.Serializable;

/**
 * This object returns details about every unit of scheduled work in the sysmte.
 */
public class RmQueriedShare
	implements Serializable
{
	private static final long serialVersionUID = -1L;

    long jobId;                            // DuccID of job
    long shareId;                          // DuccID of share
    long investmentInit;
    long investmentRt;
    int  order;
    
    boolean evicted     = false;
    boolean purged      = false;
    boolean fixed       = false;
    boolean initialized = false;

    boolean blacklisted = false;

    public RmQueriedShare(long job_id, long share_id, int order, long investment_init, long investment_rt)
    {
        this.jobId = job_id;
        this.shareId = share_id;
        this.order = order;
        this.investmentInit = investment_init;
        this.investmentRt = investment_rt;
    }

    public void setEvicted(boolean v)     { this.evicted = v; }
    public void setPurged(boolean v)      { this.purged  = v; }
    public void setFixed(boolean v)       { this.fixed   = v; }
    public void setInitialized(boolean v) { this.initialized = v; }
    public void setBlacklisted()          { this.blacklisted = true; }

    /**
     * @return the numeric ID of the request, as assigned by the Orchestrator.  Note that many
     *         shares may have the same job id, if the work is scaled-out in thecluster.
     */
    public long getJobId()          { return this.jobId; }

    /**
     * @return the unique ID of the scheduled work, as assigned by the RM.  If a job is scaled out,
     *         its processes will have the same JobId, but a different share ID as returned by this method.
     */
    public long getId()             { return this.shareId; }

    /**
     * @return the initialization investment, in millliseconds.  This is the amount of time spent thus far
     *         by the processes in its initialization phase.
     */
    public long getInvestmentInit() { return this.investmentInit; }

    /**
     * @return the runtime investment, in milliseconds.  This is the sum of the time spent by each thread
     *         in the processes, <em>on its current CAS</em>.  When the work represented by a CAS is
     *         completed, the thread contributes nothing to the investment until (or unless) it starts
     *         the process a new CAS.  The runtime investment is a heuristic used by the RM to determine
     *         which processes would lose the least amount of work if it was preempted.
     *
     *         Note that as of DUCC 2.0.0, a thread may request a reset of its investment, e.g. after
     *         it has checkpointed, allowing for very long-running CASs while still providing an accurate
     *         reflection of its investment.
     */
    public long getInvestmentRt()   { return this.investmentRt; }
    
    /**
     * @return the share-order of the processes.  This is the number of quantum shares occupied by each
     *         processes in the job.  For example, if the quantum is 15GB a 45GB process is an <em>order 3</em>
     *         process.
     */
    public int  getShareOrder()     { return order; }

    /**
     * @return true if the process has been preempted but the Orchestrator has not yet acknowledged that the
     *         process has exited, and false otherwise.
     */
    public boolean isEvicted()      { return evicted; }

    /**
     * @return true if the host the processes is running on has stopped responding, the RM has send a purge order
     *         to the Orchestrator, but has not yet received confirmation that the processes has exited, and false
     *         otherwise.
     */
    public boolean isPurged()       { return purged; }

    /**
     * @return true of the process is non-preemptable, and true otherwise.
     */
    public boolean isFixed()        { return fixed; }

    /**
     * @return true if the process has completed its initialization phase, and false otherwise.
     */
    public boolean isInitialized()  { return initialized; }


    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("{");

        sb.append("'blacklisted':");
        sb.append(blacklisted ? "True" : "False");
        sb.append(", 'jobid':");
        sb.append(Long.toString(jobId));
        sb.append(",'shareid':");
        sb.append(Long.toString(shareId));
        sb.append(",'order':");
        sb.append(Integer.toString(order));

        if ( !blacklisted ) {
            sb.append(",'investment-init':");
            sb.append(Long.toString(investmentInit));
            sb.append(",'investment-run':");
            sb.append(Long.toString(investmentRt));
            sb.append(",'evicted':");
            sb.append(evicted ? "True" : "False");
            sb.append(",'purged':");
            sb.append(purged ? "True" : "False");
            sb.append(",'fixed':");
            sb.append(fixed ? "True" : "False");
            sb.append(",'initialized':");
            sb.append(initialized ? "True" : "False");
        }
        sb.append("}");

        return sb.toString();
    }
}
