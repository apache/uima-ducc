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
package org.apache.uima.ducc.transport.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.rm.IResource;
import org.apache.uima.ducc.transport.event.rm.IRmJobState;
import org.apache.uima.ducc.transport.event.rm.IRmStateEvent;



public class RmStateDuccEvent 
    extends AbstractDuccEvent 
    implements IRmStateEvent
{
        
    private static final long serialVersionUID = -5878153925779939796L;
    
    Map<DuccId, IRmJobState> rmJobState;
    
    public RmStateDuccEvent() {
        super(EventType.RM_STATE);
    }
    
    public RmStateDuccEvent(Map<DuccId, IRmJobState> rmJobState) {
        super(EventType.RM_STATE);
        this.rmJobState = rmJobState;
    }
    
    public Map<DuccId, IRmJobState> getJobState() 
    {
        return rmJobState;
    }

    public String toString()
    {
        // Walk the rmJobStateMap
        // Each entry has state for one job. Job == job or reservation.
        // Each job has map of current resources, pending removals, and pending additions.

        // For each job, print the stable resources, pending removals, and pending additions.

        // Each resource has a DuccId which is the share ID that RM assigns, and the
        // NodeIdentity corresponding to that share.
        StringBuffer buf = new StringBuffer("\n");

        ArrayList<IRmJobState> jobs = new ArrayList<IRmJobState>();
        jobs.addAll(rmJobState.values());
        Collections.sort(jobs, new JobByIdSorter());

//         buf.append(String.format("\n%6s %8s %9s %8s %s\n", "Id", "Existing", "Additions", "Removals", "Refusal"));
//         for ( IRmJobState j : jobs ) {
//             String st = "?";
//             switch ( j.getDuccType() ) {
//                case Reservation:
//                    st = "R";
//                    break;
//                case Job:
//                    st = "J";
//                    break;
//                case Service:
//                    st = "S";
//                    break;
//             }
//             buf.append(String.format("%1s%d %8d %9d %8d %s\n",
//                                      st,
//                                      j.getId().getFriendly(),
//                                      (j.getResources() == null ? 0 : j.getResources().size()),
//                                      (j.getPendingAdditions() == null ? 0 : j.getPendingAdditions().size()),
//                                      (j.getPendingRemovals() == null ? 0 : j.getPendingRemovals().size()),
//                                      (j.isRefused() ? j.getReason() : "N/A")));
//         }

        for ( IRmJobState j : jobs ) {
            int counter = 0;
            
            if (j.isRefused() ) {
                buf.append(String.format("%s %s\n\tRefused: %s\n", j.getDuccType(), j.getId().getFriendly(), j.getReason()));
                continue;
            }

            Map<DuccId, IResource> existing = j.getResources();
            if ( existing == null ) {
                buf.append(String.format("%s %s\n\tExisting[0]", j.getDuccType(), j.getId().getFriendly()));
            } else {
                buf.append(String.format("%s %s\n\tExisting[%d]: ", j.getDuccType(), j.getId().getFriendly(), existing.size()));
                for ( IResource r : existing.values() ) {                    
                    buf.append(r.toString());
                    buf.append(" ");
                    if ((++counter % 10) == 0 ) {
                        buf.append("\n\t");
                    }

                }
                buf.append("\n");
            }

            counter = 0;

            Map<DuccId, IResource> additions = j.getPendingAdditions();
            if ( additions == null ) {
                buf.append(String.format("\tAdditions[0]"));
            } else {
                    buf.append(String.format("\tAdditions[%d]: ", additions.size()));
                for ( IResource r : additions.values() ) {
                    buf.append(r.toString());
                    buf.append(" ");
                    if ((++counter % 10) == 0 ) {
                        buf.append("\n\t");
                    }
                }
                buf.append("\n");
            }

            counter = 0;

            Map<DuccId, IResource> removals = j.getPendingRemovals();
            if ( removals == null ) {
                buf.append(String.format("\tRemovals[0]"));
            } else {
                buf.append(String.format("\tRemovals[%d]: ", removals.size()));
                for ( IResource r : removals.values() ) {
                    buf.append(r.toString());
                    buf.append(" ");
                    if ((++counter % 10) == 0 ) {
                        buf.append("\n\t");
                    }
                }
                buf.append("\n");
            }
        }
      
        return buf.toString();
    }
    //
    // Order classes by share weight, descending
    //
    static private class JobByIdSorter
        implements Comparator<IRmJobState>
    {
        public int compare(IRmJobState j1, IRmJobState j2)
        {
            return ( (int) (j1.getId().getFriendly() - j2.getId().getFriendly()) );
        }
    }

}
