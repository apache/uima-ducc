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

import java.io.Serializable;
import java.util.Arrays;

public class RmQueriedNodepool
	implements Serializable
{
	private static final long serialVersionUID = -8101741014979144426L;

    private String name;
    private int online;
    private int dead;
    private int offline;
    private int sharesAvailable;
    private int sharesFree;
    private int[] allMachines;
    private int[] onlineMachines;
    private int[] freeMachines;
    private int[] virtualMachines;

    public RmQueriedNodepool()
    {
        
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getOnline() {
		return online;
	}

	public void setOnline(int online) {
		this.online = online;
	}

	public int getDead() {
		return dead;
	}

	public void setDead(int dead) {
		this.dead = dead;
	}

	public int getOffline() {
		return offline;
	}

	public void setOffline(int offline) {
		this.offline = offline;
	}

	public int getSharesAvailable() {
		return sharesAvailable;
	}

	public void setSharesAvailable(int sharesAvailable) {
		this.sharesAvailable = sharesAvailable;
	}

	public int getSharesFree() {
		return sharesFree;
	}

	public void setSharesFree(int sharesFree) {
		this.sharesFree = sharesFree;
	}

	public int[] getAllMachines() {
		return allMachines;
	}

	public void setAllMachines(int[] allMachines) {
		this.allMachines = allMachines;
	}

	public int[] getOnlineMachines() {
		return onlineMachines;
	}

	public void setOnlineMachines(int[] onlineMachines) {
		this.onlineMachines = onlineMachines;
	}

	public int[] getFreeMachines() {
		return freeMachines;
	}

	public void setFreeMachines(int[] freeMachines) {
		this.freeMachines = freeMachines;
	}

	public int[] getVirtualMachines() {
		return virtualMachines;
	}

	public void setVirtualMachines(int[] virtualMachines) {
		this.virtualMachines = virtualMachines;
	}

    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        
        sb.append("{'name': '");
        sb.append(name);
        sb.append("',\n 'online': ");
        sb.append(Integer.toString(online));
        sb.append(",\n 'dead': ");
        sb.append(Integer.toString(dead));
        sb.append(",\n 'offline': ");
        sb.append(Integer.toString(offline));
        sb.append(",\n 'total-shares': ");
        sb.append(Integer.toString(sharesAvailable));
        sb.append(",\n 'free-shares': ");
        sb.append(Integer.toString(sharesFree));
        sb.append(",\n 'all-machines': ");
        sb.append(Arrays.toString(allMachines));
        sb.append(",\n 'online-machines': ");
        sb.append(Arrays.toString(onlineMachines));
        sb.append(",\n 'free-machines': ");
        sb.append(Arrays.toString(freeMachines));
        sb.append(",\n 'virtual-machines': ");
        sb.append(Arrays.toString(virtualMachines));
        sb.append(",\n}");

        return sb.toString();
    }

}
