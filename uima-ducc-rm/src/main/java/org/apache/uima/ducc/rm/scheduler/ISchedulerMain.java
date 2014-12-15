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

import java.util.Map;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.admin.event.RmAdminQLoadReply;
import org.apache.uima.ducc.common.admin.event.RmAdminQOccupancyReply;
import org.apache.uima.ducc.common.utils.id.DuccId;



/**
 * Define the process that manages scheduling. 
 * - Receive incoming messages (submit, cancel, etc).
 * - Invoke the IScheduler to make machine/job matching decisions
 * - Send out start and stop orders
 */
public interface ISchedulerMain
{
    public void init()
    	throws Exception;

    JobManagerUpdate schedule();
    void signalNewWork(IRmJob j);
    void signalRecovery(IRmJob j);      // job arrives, but it needs recovery

    void nodeArrives(Node n);
    void nodeDeath(Map<Node, Node> n);
    void signalCompletion(DuccId id);
    void signalInitialized(IRmJob id);
    void signalCompletion(IRmJob job, Share share);
    //void signalGrowth(DuccId jobid, Share share);

    String getDefaultFairShareName();
    String getDefaultReserveName();
    int    getDefaultNTasks();
    int    getDefaultNThreads();
    int    getDefaultMemory();

    ResourceClass getResourceClass(String name);

    IRmJob getJob(DuccId id);
    Share getShare(DuccId id);
    Machine getMachine(NodeIdentity id);

    void queryMachines();

    // two flags are needed to cope with the asynchronous messages that can arrive at any time:
    //    has the scheduler read it's config files and initialized structures?
    //    has the scheduler discovered enough resources that it can schedule work?
    boolean isInitialized();              // has scheduler read all it's config and set up its strucures?
    boolean ready();                    // have enough resources checked in so scheduler can schedule work?

    // once both initialized() and ready() occur, the RM scaffolding will enable scheduling by calling start
    void start();

    String varyoff(String[] nodes);
    String varyon(String[] nodes);
    String reconfigure();
    RmAdminQLoadReply queryLoad();
    RmAdminQOccupancyReply queryOccupancy();
}
