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
package org.apache.uima.ducc.common;

import java.util.HashMap;
import java.util.Map;


public abstract class ANodeStability
    implements Runnable
{

    private HashMap<Node, Integer> heartbeats = new HashMap<Node, Integer>();
    private int     nodeStability;
    private int     agentMetricsRate;
    private boolean shutdown = false;
    private Thread  watchThread;

    public ANodeStability(int nodeStability, int agentMetricsRate)
    {
        this.nodeStability = nodeStability;
        this.agentMetricsRate = agentMetricsRate;
    }

    /**
     * Starts the watchdog thread.
     */
    public void start()
    {
        watchThread = new Thread(this);
        watchThread.setDaemon(true);
        watchThread.start();
    }

    /**
     * This callback is invoked when node death is suspected.  You need to do your own
     * synchronization on your own implementation of this method if you need it.
     */
    public abstract void nodeDeath(Map<Node, Node> nodes);

    /**
     * Record the arrival of a node.  Implementor may want to override this to
     * perform other logic.  Be sure to call super.nodeArrives(n) if you
     * override or none of this works.
     */
    public synchronized void nodeArrives(Node n)
    {
        heartbeats.put(n, 0);
    }

    /**
     * Called if a heartbeat is missed, in case caller wants to do take some action
     * such as logging the event.  OK to do nothing. Implementor must synchronize if desired.
     */
    public abstract void missedNode(Node n, int c);

    /**
     * Graceful shutdown of the thread.
     */
    public synchronized void shutdown()
    {
        this.shutdown = true;
        watchThread.interrupt();
    }

    /**
     * Thread main, run forvever checking for delinquent nodes.
     */
    public void run()
    {
        HashMap<Node, Node> deadNodes = new HashMap<Node, Node>();
        while ( true ) {            
            synchronized(this)                
            {
                if ( shutdown ) return;

                for ( Node n : heartbeats.keySet() ) {
                    int c = heartbeats.get(n);
                    
                    if ( ++c >= nodeStability )  deadNodes.put(n,n);
                    if ( c > 1 )                 missedNode(n, c);    // tell implementor if he cares
                                                                     // note that first miss is "free"
                                                                     // because of timing, it may not be
                                                                     // a real miss
                    heartbeats.put(n, c);
                }
            }
            
            if ( deadNodes.size() > 0 ) {
                nodeDeath(deadNodes);                 // tell implementors
                for ( Node n : deadNodes.keySet() ) {          // clear from list of known nodes
                    heartbeats.remove(n);             //   so we don't keep harassing implementors
                }
                deadNodes.clear();                    // and clear our own list 
            }

            try {
				Thread.sleep(agentMetricsRate);
			} catch (InterruptedException e) {
				// nothing
			}
        }
        
    }
}
