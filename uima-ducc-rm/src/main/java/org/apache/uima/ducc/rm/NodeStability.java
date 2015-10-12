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
package org.apache.uima.ducc.rm;

import java.util.Map;

import org.apache.uima.ducc.common.ANodeStability;
import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.rm.scheduler.ISchedulerMain;
import org.apache.uima.ducc.rm.scheduler.SchedConstants;


public class NodeStability
    extends ANodeStability
    implements SchedConstants
{
    ISchedulerMain scheduler;
    ResourceManagerComponent rm;
    DuccLogger     logger = DuccLogger.getLogger(NodeStability.class, COMPONENT_NAME);

    public NodeStability(ResourceManagerComponent rm, int nodeStabilityLimit, int agentMetricsRate)
    {        
        super(nodeStabilityLimit, agentMetricsRate);
        this.rm = rm;
        this.scheduler = rm.getScheduler();
    }

    public void nodeDeath(Map<Node, Node> nodes)
    {
        String methodName = "nodeDeath";

        scheduler.nodeDeath(nodes);
        for ( Node n : nodes.keySet() ) {
            logger.debug(methodName, null, "*** ! Notification of node death:", n.getNodeIdentity().getName());
        }
    }

    public void missedNode(Node n, int c)
    {
    	String methodName = "missedNode";
        logger.warn(methodName, null, "*** Missed heartbeat ***", n.getNodeIdentity().getName(), "count[", c, "]");
        scheduler.nodeHb(n, c);
    }

    public void nodeRecovers(Node n)
    {
    	String methodName = "nodeRecovers";
        logger.info(methodName, null, "*** Node recovers ***", n.getNodeIdentity().getName());
        scheduler.nodeHb(n, 0);
    }

    public void nodeArrives(Node n)
    {
    	String methodName = "nodeArrives";
        if ( ! rm.isSchedulerReady() ) {
            logger.warn(methodName, null, "Ignoring node update, scheduler is still booting.");
            return;
        } else {
            try {
                scheduler.nodeArrives(n);          // tell RM
                super.nodeArrives(n);              // tell heartbeat monitor
            } catch ( Throwable t ) {
                logger.error(methodName, null, t);
            }
        }
    }
}
