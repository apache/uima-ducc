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

public class RmAdminQLoadReply
    extends RmAdminReply
{
	private static final long serialVersionUID = -8101741014979144426L;

    private long shareQuantum;
    private List<RmQueriedNodepool> nodepools = new ArrayList<RmQueriedNodepool>();
    private List<RmQueriedClass>    classes   = new ArrayList<RmQueriedClass>();

    public RmAdminQLoadReply()
    {
    	super(null);
    }


    public void setShareQuantum(long q)               { this.shareQuantum = q / ( 1024*1024); }
    public void addNodepool    (RmQueriedNodepool np) { nodepools.add(np); }
    public void addClass       (RmQueriedClass    cl) { classes.add(cl); }

    public long getShareQuantum()                 { return shareQuantum; }
    public List<RmQueriedNodepool> getNodepools() { return nodepools; }
    public List<RmQueriedClass>    getClasses()   { return classes; }

    public static String fmtArray(int[] array)
    {
        Object[] vals = new Object[array.length];
        StringBuffer sb = new StringBuffer();
        
        for ( int i = 0; i < array.length; i++ ) {
            sb.append("%3s ");
            vals[i] = Integer.toString(array[i]);
        }
        return String.format(sb.toString(), vals);
    }

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
    public String toCompact()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("{\n'quantum':");
        sb.append(Long.toString(shareQuantum));
        sb.append(",\n'classes': [\n");
        for ( RmQueriedClass cl : classes ) {
            sb.append(cl.toCompact());
            sb.append("\n,");
        }

        sb.append("],\n'nodepools': [\n");
        for ( RmQueriedNodepool np : nodepools ) {
            sb.append(np.toCompact());
            sb.append("\n,");
        }

        sb.append("],\n}");

        return sb.toString();
    }

    public String toConsole()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("Query Load - scheduling quantum ");
        sb.append(Long.toString(shareQuantum));
        sb.append(":\n");

        for ( RmQueriedClass cl : classes ) {
            sb.append(cl.toConsole());
            sb.append("\n");
        }

        for ( RmQueriedNodepool np : nodepools ) {
            sb.append(np.toConsole());
            sb.append("\n");
        }

        return sb.toString();
    }

}
