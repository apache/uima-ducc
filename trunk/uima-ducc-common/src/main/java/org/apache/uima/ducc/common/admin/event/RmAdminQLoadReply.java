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

import java.util.ArrayList;
import java.util.List;

/**
 * This event returns the response of a RmAdminQLoad request.
 */
public class RmAdminQLoadReply
    extends RmAdminReply
{
	private static final long serialVersionUID = 1L;

    private boolean ready = true;     // if false, RM is not initialized
    private long shareQuantum;
    private List<RmQueriedNodepool> nodepools = new ArrayList<RmQueriedNodepool>();
    private List<RmQueriedClass>    classes   = new ArrayList<RmQueriedClass>();

    public RmAdminQLoadReply()
    {
    	super();
    }


    /* RM only, other use produces incorrect results. */
    public void setShareQuantum(long q)               { this.shareQuantum = q / ( 1024*1024); }
    /* RM only, other use produces incorrect results. */
    public void addNodepool    (RmQueriedNodepool np) { nodepools.add(np); }
    /* RM only, other use produces incorrect results. */
    public void addClass       (RmQueriedClass    cl) { classes.add(cl); }

    /**
     * @return the share quantum currently being used by RM.
     */
    public long getShareQuantum()                 { return shareQuantum; }

    /**
     * @return the {@link RmQueriedNodepool nodepool} details.
     */
    public List<RmQueriedNodepool> getNodepools() { return nodepools; }

    /**
     * @return The {@link RmQueriedNodepool class} details.
     */
    public List<RmQueriedClass>    getClasses()   { return classes; }

    /* RM only, other use produces incorrect results. */
    public void    notReady()                     { this.ready = false; }

    /**
     * @return True if RM is able to schedule and be queried, false otherwise. If the RM is not yet
     * ready to schedule, e.g. immediately after boot or reconfigure, this method will return false.
     */
    public boolean isReady()                      { return ready; }

    /**
        The compact format creates a Python structure of this form:
        A dictionary with these keys:
            quantum
               value is a single integer
            classes
               value is a list of dictionaries describing demand
                       with these keys
                           name
                              value is string, the name of the class
                           policy
                              value is the scheduling policy
                           requested
                              value is a list of integers
                           awarded
                              value is a list of integers
            nodepools
               value is a list of dictionaries describing a node pool
                        with these keys
                           name
                              value is the namne of the nodepool
                           online
                              value is an integer
                           dead
                              value is an integer
                           offline
                              value is an integer
                           total-shares
                              value is an integer
                           free-shares
                              value is an integer
                           all-machines
                              value is a list of integers
                           online-machines
                              value is a list of integers
                           free-machines
                              value is a list of integers
                           virtual-machines
                              value is a list of integers
    */
    public String toString()
    {

        if ( !ready ) {
            return "RM is not yet initialized.";
        }

        StringBuffer sb = new StringBuffer();
        sb.append("{\n'quantum':");
        sb.append(Long.toString(shareQuantum));
        sb.append(",\n'classes': [\n");
        for ( RmQueriedClass cl : classes ) {
            sb.append(cl.toString());
            sb.append("\n,");
        }

        sb.append("],\n'nodepools': [\n");
        for ( RmQueriedNodepool np : nodepools ) {
            sb.append(np.toString());
            sb.append("\n,");
        }

        sb.append("],\n}");

        return sb.toString();
    }
}
