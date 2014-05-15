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

public class RmAdminQLoadReply
    extends DuccAdminEvent 
{
	private static final long serialVersionUID = -8101741014979144426L;

    int nodesOnline;      // number of schedulable nodes
    int nodesDead;        // number of nodes marked dead
    int nodesOffline;     // number of nodes varied off
    int nodesFree;        // number of nodes with nothing on them

    int sharesAvailable;  // total shares available for scheduling (busy plus empty)
    int sharesFree;       // total shares unused
    int sharesDemanded;   // shares that would be used in an infinite-share system

    int pendingEvictions;  // number of evictions started but not confirmed
    int pendingExpansions; // number of expansions started but not confirmed

    public RmAdminQLoadReply()
    {
    }

    public int getNodesOnline() {
		return nodesOnline;
	}

	public void setNodesOnline(int nodes_online) {
		this.nodesOnline = nodes_online;
	}

	public int getNodesFree() {
		return nodesFree;
	}

	public void setNodesFree(int nodes_free) {
		this.nodesFree = nodes_free;
	}

	public int getNodesDead() {
		return nodesDead;
	}

	public void setNodesDead(int nodes_dead) {
		this.nodesDead = nodes_dead;
	}


	public int getNodesOffline() {
		return nodesOffline;
	}

	public void setNodesOffline(int nodes_offline) {
		this.nodesOffline = nodes_offline;
	}

	public int getSharesAvailable() {
		return sharesAvailable;
	}

	public void setSharesAvailable(int shares_available) {
		this.sharesAvailable = shares_available;
	}

	public int getSharesFree() {
		return sharesFree;
	}

	public void setSharesFree(int shares_free) {
		this.sharesFree = shares_free;
	}

	public int getPendingEvictions() {
		return pendingEvictions;
	}

	public void setPendingEvictions(int pending_evictions) {
		this.pendingEvictions = pending_evictions;
	}

	public int getPendingExpansions() {
		return pendingExpansions;
	}

	public void setPendingExpansions(int pending_expansions) {
		this.pendingExpansions = pending_expansions;
	}

	public int getSharesDemanded() {
		return sharesDemanded;
	}

	public void setSharesDemanded(int shares_demanded) {
		this.sharesDemanded = shares_demanded;
	}

    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        String hdr = "%10s %9s %12s %9s %16s %10s %14s %16s %17s";
        String fmt = "%10d %9d %12d %9d %16d %10d %14d %16d %17d";
        sb.append(String.format(hdr, "TotalNodes", "DeadNOdes", "OfflineNodes", "FreeNodes",
                                         "SharesAvailable", "SharesFree", "SharesDemanded",
                                "PendingEvictions", "PendingExpansions"));
        sb.append(String.format(fmt, nodesOnline, nodesDead, nodesOffline, nodesFree,
                  sharesAvailable, sharesFree, sharesDemanded,
                  pendingEvictions, pendingExpansions));
        return sb.toString();
    }

}
