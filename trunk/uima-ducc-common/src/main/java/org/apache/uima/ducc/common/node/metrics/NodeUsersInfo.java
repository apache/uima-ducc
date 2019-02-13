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
package org.apache.uima.ducc.common.node.metrics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.uima.ducc.common.utils.id.IDuccId;


public class NodeUsersInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private String uid;
  private List<NodeProcess> reserveProcesses = new ArrayList<NodeProcess>();
	private transient List<IDuccId> reservations = new ArrayList<IDuccId>();
	private List<NodeProcess> rogueProcesses = new ArrayList<NodeProcess>();
  private AtomicInteger counter = new AtomicInteger(1);
  
	public NodeUsersInfo(String uid ) {
	  this.uid = uid;
	}

	public boolean markAsRogue(int ceiling) {
	  if ( counter.get() < ceiling ) {
	    counter.addAndGet(1);
	    return false;
	  } else {
	    return true;
	  }
	}
  public String getUid() {
    return uid;
  }

  public List<IDuccId> getReservations() {
    return reservations;
  }

  public List<NodeProcess> getRogueProcesses() {
    return rogueProcesses;
  }
	
  public void addPid(String pid, String ppid, boolean java) {
    reserveProcesses.add(new NodeProcess(pid,ppid,java));

  }
  public void addReservation(IDuccId reservation) {
    reservations.add(reservation);
  }
  public void addReservation(List<IDuccId> reservations) {
    this.reservations = reservations;
  }
  public void addRogueProcess(String roguePID, String ppid, boolean isJava ) {
    rogueProcesses.add(new NodeProcess(roguePID, ppid, isJava));
  }
  public String toString() {
    if ( reservations.size() == 0 && rogueProcesses.size() == 0 ) {
      return "";
    }
    StringBuffer sb = new StringBuffer();
    String reservation = ( reservations.size() > 0 ) ? "Y" : "N";
    String userId = getUid();
    String rogue = ( reservations.size() == 0 ) ? "Y" : "N";
    
    sb.append(String.format("\n%32s%20s%8s%12s%12s\n","UserID","Reservation","Rogue","Java","NonJava"));
    sb.append(String.format("%32s%20s%8s%12s%12s\n",StringUtils.center("", 30, "-"),StringUtils.center("", 18, "-"),StringUtils.center("", 6, "-"),StringUtils.leftPad("", 10, "-"),StringUtils.leftPad("", 10, "-")));
    if ( reservations.size() > 0 ) {
      for( NodeProcess process : reserveProcesses) {
        
        if ( process.isJava()) {
          sb.append(String.format("%32s%20s%8s%12s%12s\n",userId,reservation,rogue,process.getPid(),""));
        } else {
          sb.append(String.format("%32s%20s%8s%12s%12s\n",userId,reservation,rogue,"",process.getPid()));
        }
        userId = "";
        reservation = "";
      }
    } else {
      for( NodeProcess process : rogueProcesses) {
        if ( process.isJava()) {
          sb.append(String.format("%32s%20s%8s%12s%12s\n",userId,reservation,rogue,process.getPid(),""));
        } else {
          sb.append(String.format("%32s%20s%8s%12s%12s\n",userId,reservation,rogue,"",process.getPid()));
        }
        userId = "";
        reservation = "";
      }
    }
    sb.append("\n");

    return sb.toString();
  }
  public class NodeProcess implements Serializable {
    private static final long serialVersionUID = 1L;
    boolean java;
    String pid;
    String ppid;
    

	public NodeProcess(String pid, String ppid, boolean isJava) {
      this.pid = pid;
      java = isJava;
    }
    public String getPpid() {
		return ppid;
	}
	public void setPpid(String ppid) {
		this.ppid = ppid;
	}
	
    public boolean isJava() {
      return java;
    }

    public void setJava(boolean java) {
      this.java = java;
    }

    public String getPid() {
      return pid;
    }

    public void setPid(String pid) {
      this.pid = pid;
    }
    
  }
  public List<NodeProcess> getReserveProcesses() {
    return reserveProcesses;
  }
}
