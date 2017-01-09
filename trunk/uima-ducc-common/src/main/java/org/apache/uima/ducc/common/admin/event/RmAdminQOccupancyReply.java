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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This event returns detailed information on the scheduling state of each active host.
 */
public class RmAdminQOccupancyReply
    extends RmAdminReply
{
	private static final long serialVersionUID = 1L;

    boolean ready = true;         // If not ready, RM is not initialized
    List<RmQueriedMachine> machines = new ArrayList<RmQueriedMachine>();

    public RmAdminQOccupancyReply()
    {
    	super();
    }

    public void addMachine(RmQueriedMachine m) 
    {
        machines.add(m);
    }

    /**
     * Used by RM only.
     */
    public void    notReady()   { this.ready = false; }

    /**
     * @return True if RM is able to schedule and be queried, false otherwise. If the RM is not yet
     * ready to schedule, e.g. immediately after boot or reconfigure, this method will return false.
     */
    public boolean isReady()    { return ready; }

    public List<RmQueriedMachine> getMachines()
    {
        return machines;
    }

    public String toString()
    {

        if ( !ready ) {
            return "RM is not yet initialized.";
        }

        Collections.sort(machines, new MachineByMemorySorter());
        StringBuffer sb = new StringBuffer();

        sb.append("[\n");
        for ( RmQueriedMachine m : machines ) {
            sb.append(m.toString());
            sb.append(",\n");
        }
        sb.append("]");
        return sb.toString();
    }

    class MachineByMemorySorter
    	implements Comparator<RmQueriedMachine>
    {	
    	public int compare(RmQueriedMachine m1, RmQueriedMachine m2)
        {
            long mm1 = m1.getMemory();
            long mm2 = m2.getMemory();
            if ( mm1 == mm2 ) {
                return m1.getId().compareTo(m2.getId());
            }
            return (int) (mm2 - mm1);
        }
    }


}
