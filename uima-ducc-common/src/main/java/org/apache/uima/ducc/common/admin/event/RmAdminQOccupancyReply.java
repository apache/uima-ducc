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

public class RmAdminQOccupancyReply
    extends RmAdminReply
{
	private static final long serialVersionUID = -8101741014979144426L;


    List<RmQueriedMachine> machines = new ArrayList<RmQueriedMachine>();

    public RmAdminQOccupancyReply()
    {
    	super(null);
    }

    public void addMachine(RmQueriedMachine m) 
    {
        machines.add(m);
    }

    public List<RmQueriedMachine> getMachines()
    {
        return machines;
    }


    public String toConsole()
    {
        Collections.sort(machines, new MachineByOrderSorter());
        StringBuffer sb = new StringBuffer();
        sb.append(RmQueriedMachine.header());
        sb.append("\n");
        sb.append(RmQueriedMachine.separator());
        sb.append("\n");
        for ( RmQueriedMachine m : machines ) {
            sb.append(m.toConsole());
            sb.append("\n");
        }
        return sb.toString();
    }

    public String toCompact()
    {
        // ( no need to sort, this is intended for script scraping so we let the script order things as it wants )
        StringBuffer sb = new StringBuffer();
        for ( RmQueriedMachine m : machines ) {
            sb.append(m.toCompact());
            sb.append("\n");
        }
        return sb.toString();
    }

    class MachineByOrderSorter
    	implements Comparator<RmQueriedMachine>
    {	
    	public int compare(RmQueriedMachine m1, RmQueriedMachine m2)
        {
            int o1 = m1.getShareOrder();
            int o2 = m2.getShareOrder();
            if ( o1 == o2 ) {
                return m1.getId().compareTo(m2.getId());
            }
            return o2 - o1;
        }
    }


}
