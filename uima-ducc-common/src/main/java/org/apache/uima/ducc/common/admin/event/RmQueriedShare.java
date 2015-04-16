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

    public long getJobId()          { return this.jobId; }
    public long getId()             { return this.shareId; }
    public long getInvestmentInit() { return this.investmentInit; }
    public long getInvestmentRt()   { return this.investmentRt; }
    public int  getShareOrder()     { return order; }

    public boolean isEvicted()      { return evicted; }
    public boolean isPurged()       { return purged; }
    public boolean isFixed()        { return fixed; }
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
