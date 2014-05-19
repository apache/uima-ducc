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
    extends RmAdminReply
{
	private static final long serialVersionUID = -8101741014979144426L;

    int nodesOnline;      // number of schedulable nodes
    int nodesDead;        // number of nodes marked dead
    int nodesOffline;     // number of nodes varied off
    int nodesFree;        // number of nodes with nothing on them

    int sharesAvailable;  // total shares available for scheduling (busy plus empty)
    int sharesFree;       // total shares unused
    int[] sharesDemanded; // shares that would be used in an infinite-share system
    int[] sharesAwarded;  // shares assigned, less pending preemptions, plus pending expansions
    int[] machinesOnline;   // machines available for scheduling
    int[] machinesFree; // machines with no assignments
    int[] machinesVirtual; // machines after carving out space
    int pendingEvictions;  // number of evictions started but not confirmed
    int pendingExpansions; // number of expansions started but not confirmed

    public RmAdminQLoadReply()
    {
    	super(null);
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

	public int[] getSharesDemanded() {
		return sharesDemanded;
	}

	public void setSharesDemanded(int[] shares_demanded) {
		this.sharesDemanded = shares_demanded;
	}

	public int[] getSharesAwarded() {
		return sharesAwarded;
	}

	public void setSharesAwarded(int[] shares_awarded) {
		this.sharesAwarded = shares_awarded;
	}

	public int[] getMachinesOnline() {
		return machinesOnline;
	}

	public void setMachinesOnline(int[] machines_online) {
		this.machinesOnline = machines_online;
	}

	public int[] getMachinesFree() {
		return machinesFree;
	}

	public void setMachinesFree(int[] machines_free) {
		this.machinesFree = machines_free;
	}

	public int[] getVirtualMachinesFree() {
        int[] vm = machinesVirtual.clone();
        int len = vm.length;
        for ( int o = 1; o < len; o++ ) {                     // counting by share order
            //nFreeSharesByOrder[o] = nMachinesByOrder[o] * o;
            for ( int p = o+1; p < len; p++ ) {
                if ( vm[p] != 0 ) {
                    vm[o] += (p / o) * vm[p];
                }
            }
        }

		return vm;
	}

	public int[] getMachinesVirtual() {
		return this.machinesVirtual;
	}

	public void setMachinesVirtual(int[] machines_virtual) {
		this.machinesVirtual = machines_virtual;
	}

    private String fmtArray(int[] array)
    {
        Object[] vals = new Object[array.length];
        StringBuffer sb = new StringBuffer();
        
        for ( int i = 0; i < array.length; i++ ) {
            sb.append("%3s ");
            vals[i] = Integer.toString(array[i]);
        }
        return String.format(sb.toString(), vals);
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        String hdr = "%10s %9s %12s %9s %16s %10s %16s %17s";
        String fmt = "%10d %9d %12d %9d %16d %10d %16d %17d";
        sb.append(String.format(hdr, "TotalNodes", "DeadNOdes", "OfflineNodes", "FreeNodes",
                                         "SharesAvailable", "SharesFree", 
                                "PendingEvictions", "PendingExpansions"));
        sb.append("\n");
        sb.append(String.format(fmt, nodesOnline, nodesDead, nodesOffline, nodesFree,
                  sharesAvailable, sharesFree, 
                  pendingEvictions, pendingExpansions));

        sb.append("\nOnline Hosts By Order:\n");
        sb.append(fmtArray(machinesOnline));

        sb.append("\nFree Hosts By Order:\n");
        sb.append(fmtArray(machinesFree));

        sb.append("\nFree Shares By Order:\n");
        sb.append(fmtArray(machinesVirtual));

        sb.append("\nFree Virtual Shares By Order:\n");
        sb.append(fmtArray(getVirtualMachinesFree()));

        sb.append("\nWanted Shares By Order:\n");
        sb.append(fmtArray(sharesDemanded));

        sb.append("\nAwarded Shares By Order:\n");
        sb.append(fmtArray(sharesAwarded));
        return sb.toString();
    }

}
