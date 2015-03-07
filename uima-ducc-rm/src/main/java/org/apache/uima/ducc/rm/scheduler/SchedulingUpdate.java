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

public class SchedulingUpdate
{
    private HashMap<IRmJob, IRmJob> shrunken;
    private HashMap<IRmJob, IRmJob> expanded;
    private HashMap<IRmJob, IRmJob> stable;
    private HashMap<IRmJob, IRmJob> dormant;
    private HashMap<IRmJob, IRmJob> reservations;
    private HashMap<IRmJob, IRmJob> refusals;

    public SchedulingUpdate()
    {
        shrunken     = new HashMap<IRmJob, IRmJob>();
        expanded     = new HashMap<IRmJob, IRmJob>();
        stable       = new HashMap<IRmJob, IRmJob>();
        dormant      = new HashMap<IRmJob, IRmJob>();
        reservations = new HashMap<IRmJob, IRmJob>();
        refusals     = new HashMap<IRmJob, IRmJob>();
    }

//     public SchedulingUpdate(
//                             ArrayList<Job> shrunken,
//                             ArrayList<Job> expanded,
//                             ArrayList<Job> leftovers)
//     {
//         this.shrunken = shrunken;
//         this.expanded = expanded;
//         this.leftovers = leftovers;
//     }

    void addShrunkenJob(IRmJob j)
    {
        shrunken.put(j, j);
    }

    void addExpandedJob(IRmJob j)
    {
        expanded.put(j, j);
    }

    void addDormantJob(IRmJob j)
    {
        dormant.put(j, j);
    }

    void addStableJob(IRmJob j)
    {
        stable.put(j, j);
    }

     void addReservation(IRmJob j)
     {
         reservations.put(j, j);
     }

    void refuse(IRmJob j, String reason)
    {
        j.refuse(reason);
        refusals.put(j, j);
    }

    void defer(IRmJob j, String reason)
    {
        j.defer(reason);
    }

    HashMap<IRmJob, IRmJob> getRefusedJobs() 
    {
        return refusals;
    }

    HashMap<IRmJob, IRmJob> getReservedJobs() 
    {
        return reservations;
    }

    HashMap<IRmJob, IRmJob> getShrunkenJobs() 
    {
        return shrunken;
    }

    HashMap<IRmJob, IRmJob> getExpandedJobs() 
    {
        return expanded;
    }

    HashMap<IRmJob, IRmJob> getStableJobs() 
    {
        return stable;
    }

    HashMap<IRmJob, IRmJob> getDormantJobs() 
    {
        return dormant;
    }

//     HashMap<IRmJob, IRmJob> getReservations() 
//     {
//         return reservations;
//     }

    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append("Expanded:\n");
        if ( expanded.size() == 0 ) {
            sb.append("<none>\n");
        } else {
            sb.append("   ");
            sb.append(RmJob.getHeader());
            sb.append("\n");
            for (IRmJob j : expanded.values()) {
                sb.append("   ");
                sb.append(j.toString());
                sb.append("\n");
            }
        }

        sb.append("\nShrunken:\n");
        if ( shrunken.size() == 0 ) {
            sb.append("   <none>\n");
        } else {
            sb.append("   ");
            sb.append(RmJob.getHeader());
            sb.append("\n");
            for (IRmJob j : shrunken.values()) {
                sb.append("   ");
                sb.append(j.toString());
                sb.append("\n");
            }
        }

        sb.append("\nStable:\n");
        if ( stable.size() == 0 ) {
            sb.append("   <none>\n");
        } else {
            sb.append("   ");
            sb.append(RmJob.getHeader());
            sb.append("\n");
            for (IRmJob j : stable.values()) {
                sb.append("   ");
                sb.append(j.toString());
                sb.append("\n");
            }
        }

        sb.append("\nDormant:\n");
        if ( dormant.size() == 0 ) {
            sb.append("   <none>\n");
        } else {
            sb.append("   ");
            sb.append(RmJob.getHeader());
            sb.append("\n");
            for (IRmJob j : dormant.values()) {
                sb.append("   ");
                sb.append(j.toString());
                sb.append("\n");
            }
        }

        sb.append("\nReserved:\n");
        if ( reservations.size() == 0 ) {
            sb.append("   <none>\n");
        } else {
            sb.append("   ");
            sb.append(RmJob.getHeader());
            sb.append("\n");
            for (IRmJob j : reservations.values()) {
                sb.append("   ");
                sb.append(j.toString());
                sb.append("\n");
            }
        }

        sb.append("\n");

        return sb.toString();
    }
}
