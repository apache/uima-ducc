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
import java.util.ArrayList;
import java.util.List;

/**
 * This object represents details of a host as seen by the Resource Manager.
 */
public class RmQueriedMachine
	implements Serializable
{
	private static final long serialVersionUID = 1L;

    String name;
    String nodepoolId;
    long memory;
    int order;
    boolean blacklisted;                                         // UIMA-4142
    boolean online;                                              // UIMA-4234
    boolean responsive;                                          // UIMA-4234

    List<RmQueriedShare> shares = null;
    
    // UIMA-4142, account for blacklist
    public RmQueriedMachine(String name, String nodepoolId, long memory, int order, boolean blacklisted)
    {
        this.name = name;
        this.nodepoolId = nodepoolId;
        this.memory = memory;
        this.order = order;
        this.blacklisted = blacklisted;
        this.online = true;
        this.responsive = true;
    }

    public void setOffline()      { this.online = false; }       // UIMA-4234
    public void setUnresponsive() { this.responsive = false; }   // UIMA-4234

    public void addShare(RmQueriedShare rqs)
    {
        if ( shares == null ) shares = new ArrayList<RmQueriedShare>();
        shares.add(rqs);
    }

    /**
     * Return the list of shares scheduled to this host.
     * @return The list of {@link RmQueriedShare shares}.
     */
    public List<RmQueriedShare> getShares()
    {
        return shares;
    }

    /**
     * Returns the name of the machin.
     * @return the name of the machine.
     */
    public String getId()           { return name; }
    
    /**
     * Returns the amount of RAM in KB (kilobytes) on the machine.
     * @return the amount of RAM in the machine, in kilobytes.
     */
    public long getMemory()         { return memory; }

    /**
     * Returns the share order of the machine.
     *
     * @return the number of quantum shares supported by the machine.  For example, if the share quantum is
     *         15GB, a 48GB machine is of order 3.
     */
    public int getShareOrder()      { return order; }

    /**
     * Returns whether the machine is blacklisted.
     *
     * @return whether the host contains blacklisted processes.  A blacklisted process is one whose characteristics
     *         no longer match the machine.  For example, after reconfiguration, this may be an illegal assignemnt
     *         for the request.  Until such time as the request is (externally) removed, the RM has to account for
     *         the space by maintaining records on the blacklisted work.  If a machine is blacklisted, it is not
     *         schedulable for new work.
     */
    public boolean isBlacklisted()  { return blacklisted; }        // UIMA-4142

    /**
     * Returns whether the machine is varied online.
     *
     * @return true if the machine is varied online and false otherwise.  Note that a machine might "not varied offline",
     *         but not responsive (not sending heartbeats).
     */
    public boolean isOnline()       { return online; }             // UIMA-4142

    /**
     * Indicates whether the machine is sending heartbeats.
     * @return true if the machine is sending heartbeats.  Note that a machine might be sending heartbeats but be varied
     *         offline or blacklisted, and hence not schedulable.
     */
    public boolean isResponsive()   { return responsive; }         // UIMA-4142
    
    public String toString() 
    {
        // name memory order(nqshares) unused-shares share-details...
        StringBuffer sb = new StringBuffer();
        sb.append("{'name':'");
        sb.append(name);
        sb.append("','nodepool':'");
        sb.append(nodepoolId);
        sb.append("','blacklisted':");
        sb.append(blacklisted ? "True" : "False");
        sb.append(",'online':");
        sb.append(online ? "True" : "False");
        sb.append(",'status': ");
        sb.append(responsive ? "'up'" : "'down'");
        sb.append(",'nodepool': '");
        sb.append(nodepoolId);
        sb.append("','memory':");
        sb.append(Long.toString(memory));
        sb.append(",'order':");
        sb.append(Integer.toString(order));
        
        if ( shares == null ) {
            sb.append(",'shares-free':'");
            sb.append(Integer.toString(order));
            sb.append("'");
            sb.append(",'shares':[],\n");
        } else {
            int used = 0;
            for ( RmQueriedShare s : shares ) {                
                used += s.getShareOrder();
            }

            sb.append(",'shares-free':");
            sb.append(Integer.toString(order - used));
            sb.append(",'shares':[");
            for ( RmQueriedShare s : shares ) {
                sb.append(s.toString());
                sb.append(",");
            }
            sb.append("],\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

}
