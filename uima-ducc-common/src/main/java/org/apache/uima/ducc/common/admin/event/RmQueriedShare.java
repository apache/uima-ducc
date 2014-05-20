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

public class RmQueriedShare
	implements Serializable
{
	private static final long serialVersionUID = -8101741014979144426L;

    long jobId;                            // DuccID of job
    long shareId;                          // DuccID of share
    long investmentInit;
    long investmentRt;
    int  order;
    
    boolean evicted     = false;
    boolean purged      = false;
    boolean fixed       = false;
    boolean initialized = false;
    public RmQueriedShare(long job_id, long share_id, int order, long investment_init, long investment_rt)
    {
        this.jobId = job_id;
        this.shareId = share_id;
        this.order = order;
        this.investmentInit = investment_init;
        this.investmentRt = investment_rt;
    }

    public void setEvicted(boolean v){ this.evicted = v; }
    public void setPurged(boolean v){ this.purged  = v; }
    public void setFixed(boolean v) { this.fixed   = v; }
    public void setInitialized(boolean v) {this.initialized = v; }

    public long getJobId()          { return this.jobId; }
    public long getId()             { return this.shareId; }
    public long getInvestmentInit() { return this.investmentInit; }
    public long getInvestmentRt()   { return this.investmentRt; }
    public int  getShareOrder()     { return order; }

    public boolean isEvicted()      { return evicted; }
    public boolean isPurged()       { return purged; }
    public boolean isFixed()        { return fixed; }
    public boolean isInitialized()  { return initialized; }


    public String toCompact()
    {
        return String.format("%d %d %d %d %d %s %s %s", jobId, shareId, order, investmentInit, investmentRt, evicted, purged, fixed, initialized);
    }

    public String toConsole()
    {
        return String.format("J[%8d] S[%8d] O[%d] II[%8d] IR[%8d] E[%5s] P[%5s] F[%5s] I[%5s]", jobId, shareId, order, investmentInit, investmentRt, evicted, purged, fixed, initialized);
    }
}
