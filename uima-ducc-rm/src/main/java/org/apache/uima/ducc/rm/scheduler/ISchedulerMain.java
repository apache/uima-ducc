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

    String getDefaultClassName();
    int    getDefaultNTasks();
    int    getDefaultNThreads();
    int    getDefaultMemory();

    ResourceClass getResourceClass(String name);

    IRmJob getJob(DuccId id);
    Share getShare(DuccId id);
    Machine getMachine(NodeIdentity id);

    void queryMachines();

    boolean ready();                    // we don't accept any state until scheduler is ready
    void start();
}
