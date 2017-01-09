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
package org.apache.uima.ducc.common.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.uima.ducc.common.utils.Utils;

public class DuplicateDuccDaemonProcessDetector {
	private static String thisProcessPID = Utils.getPID();
	private static String ducc_user = System.getProperty("user.name");
	private static final String brokerProcessId = "-Dactivemq.base";
	private static final String duccDaemonId = "-Dducc.deploy.components";
	private static final String[] processCollectorCommandLine = 
			new String[] {"ps","-Ao","user:12,pid,ppid,uid,args", "--no-heading"};

	
	public enum DuccDaemonType {
		
		AGENT("agent"),OR("orchestrator"),
		RM("rm"),SM("sm"),BROKER("broker"),
		WS("ws"), PM("pm"),NotDuccDaemon("");
		private final String daemonType;

		private DuccDaemonType(String type) {
			daemonType = type;
		}
		
		public String getName() {
			return daemonType;
		}
	}
	public static void main(String[] args) {

		DuplicateDuccDaemonProcessDetector processCollector =
				new DuplicateDuccDaemonProcessDetector();
		String thisProcessDaemonType = System.getProperty(duccDaemonId.substring(2)); // skip -D
		processCollector.isThisProcessDuplicateDaemon(thisProcessDaemonType);  
	}
	/**
	 * Checks if this process is another instance of PM, OR, SM, RM, WS, or Agent.
	 * The JD, JP, and Services are excluded from the check although the bootstrapping
	 * code is the same as the real daemons. 
	 * 
	 * @param daemonName
	 * @return
	 */
	public boolean isThisProcessDuplicateDaemon(String daemonName ) {
		// Convert this daemon type ("or", "sm", etc) to enumeration type.
		// Returns NotDuccDaemon if this process is not a true ducc daemon,
		// WS, RM, PM, SM, OR, or Agent.
		DuccDaemonType thisDuccDaemonType = getDaemonTypeForName(daemonName);
		// If cant convert to enumeration type, this process is not a ducc daemon
		if ( !thisDuccDaemonType.equals(DuccDaemonType.NotDuccDaemon)) {
			return isThisProcessDuplicateDaemon(thisDuccDaemonType);
		}
		return false;
	}
	/**
	 * Launches ps command and scrapes a list of processes currently running on a node
	 * to detect if this process is a duplicate of already running Ducc daemon.
	 * 
	 * @param thisDuccDaemonType - this process daemon type (OR, WS, PM, etc)
	 * 
	 * @return true if this process is a duplicate of a running ducc daemon, otherwise false.
	 */
	public boolean isThisProcessDuplicateDaemon(DuccDaemonType thisDuccDaemonType) {
		try {
			// launch 'ps' command and return a list of running processes on this node
			List<RunningProcess> allProcesses = 
					getAllProcessesRunningOnNodeNow();
			
			for( RunningProcess process : allProcesses ) {
				// exclude this process from the check
				if ( thisProcessPID.equals(process.getPid()) ) {
					continue; // skip this process
				}
				// only care about current user processes
				if (ducc_user.equals(process.getUserName()) ) {
					// extract process type ("ws", "or",etc. Returns NotDuccDaemon otherwise.
					// It checks the value of -Dducc.deploy.components property to determine
					// if it is a daemon process.
					DuccDaemonType runningDuccDaemonType = 
							duccDaemonType(process.getCommandline());
					// Only care about Ducc daemon processes
					if (DuccDaemonType.NotDuccDaemon.equals(runningDuccDaemonType)) {
						continue; // skip
					} else if ( thisDuccDaemonType.equals(runningDuccDaemonType) ) {
						/****** FOUND DUPLICATE DUCC DAEMON ******/
						System.out.println("The Ducc daemon:"+runningDuccDaemonType.getName()+" is already runninig - duplicates are not allowed");
						return true;
					} else {
					//	System.out.println("Process PID:"+process.getPid()+" Ducc Daemon:"+runningDuccDaemonType.getName());
					}
				}
			}
		} catch( Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public List<RunningProcess> getAllProcessesRunningOnNodeNow() throws RuntimeException {
		BufferedReader reader = null;
		List<RunningProcess> nodeProcessList = new ArrayList<RunningProcess>();
		try {
			InputStream inputStream = launchProcessCollector(processCollectorCommandLine);
		    reader = new BufferedReader(new InputStreamReader(inputStream));
		    String line;
		    String regex = "\\s+";
		    
	        // read the next line from ps output
		    while ((line = reader.readLine()) != null) {
		      String tokens[] = line.split(regex);
		      if ( tokens.length > 0 ) {
		    	 // copy the whole command line starting at index 4 
		    	 String[] commandLine = Arrays.copyOfRange(tokens, 4, tokens.length);
		    	  
		       	 RunningProcess p = 
		                      new RunningProcess().
		                      withUserName(tokens[0]).
		                      withPid(tokens[1]).
		                      withParentPid(tokens[2]).
		                      withUserId(tokens[3]).
		                      withCommandline(commandLine);
		       	 
		            // add process to a list which is used to look up each process parent
		         nodeProcessList.add(p);
		      }
		    }
		} catch( Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch( IOException ioe) {
			}
		}
		return nodeProcessList;
	}
	
	private InputStream launchProcessCollector(String[] processCommandLine) throws Exception {
		InputStream inputStream = null;
	
        ProcessBuilder pb = new ProcessBuilder(processCommandLine);
	    pb.redirectErrorStream(true);
	    Process proc = pb.start();
	    //  spawn ps command and scrape the output
	    inputStream = proc.getInputStream();
        return inputStream;
	}
	private DuccDaemonType getDaemonTypeForName(String aName) {
    	for( DuccDaemonType dt : DuccDaemonType.values() ) {
    		if ( dt.getName().equals(aName)) {
    			return dt;
    		}
    	}
    	return DuccDaemonType.NotDuccDaemon;
	}
	  private DuccDaemonType duccDaemonType(String[] tokens) {
		  DuccDaemonType daemonType = DuccDaemonType.NotDuccDaemon;
		  
		  for( String token : tokens ) {
		      if ( token.startsWith(duccDaemonId)) {
		        int pos = token.indexOf("=");
		        if ( pos > -1 ) {
		        	String daemon = token.substring(pos+1);
		        	for( DuccDaemonType dt : DuccDaemonType.values() ) {
		        		if ( dt.getName().equals(daemon)) {
		        			return dt;
		        		}
		        	}
		        }
		      } else if (token.startsWith(brokerProcessId)) {
		    	  daemonType = DuccDaemonType.BROKER;
		    	  break;
		      }
		    }
		    return daemonType;
		  }

	  private class RunningProcess {
		  String pid;
		  String userName;
		  String userId;
		  String parentPid;
		  String[] commandline;
		  
		  
		public String getPid() {
			return pid;
		}

		protected RunningProcess withPid(String pid) {
			this.pid = pid;
			return this;
		}

		public String getUserName() {
			return userName;
		}

		protected RunningProcess withUserName(String userName) {
			this.userName = userName;
			return this;
		}

		@SuppressWarnings("unused")
		public String getUserId() {
			return userId;
		}

		protected RunningProcess withUserId(String userId) {
			this.userId = userId;
			return this;
		}

		@SuppressWarnings("unused")
		public String getParentPid() {
			return parentPid;
		}

		protected RunningProcess withParentPid(String parentPid) {
			this.parentPid = parentPid;
			return this;
		}

		public String[] getCommandline() {
			return commandline;
		}

		protected RunningProcess withCommandline(String[] commandline) {
			this.commandline = commandline;
			return this;
		}
	  }
}
