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
package org.apache.uima.ducc.transport.dispatcher;

import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;

/** 
 * This class is responsible for sending process state updates to
 * a remote listener process (Agent). This update is send as a String
 * through a socket. The listener process socket port is identified by 
 * an env var DUCC_STATE_UPDATE_PORT.
 * The update String syntax is:
 * 
 * DUCC_PROCESS_UNIQUEID=XXX,DUCC_PROCESS_STATE=YYY
 * 
 * where
 *    XXX is a unique process ID obtained from env var DUCC_PROCESS_UNIQUEID
 *    YYY is one of two states: Initializing or Running
 * 
 */
public class ProcessStateDispatcher {
	/*
	 * Establish connection with a remote listener process via
	 * a socket using a port from env var DUCC_STATE_UPDATE_PORT
	 */
    private Socket connectWithAgent() throws Exception {
    	InetAddress host = null;
        int statusUpdatePort = -1;
    	
    	host = InetAddress.getLocalHost();
    	String port = System.getenv("DUCC_STATE_UPDATE_PORT");
    	if ( port == null ) {
    	} else {
    		try {
       		   statusUpdatePort = Integer.valueOf(port);
   		    } catch( NumberFormatException nfe) {
    		}
    	}
    	System.out.println("Service Connecting Socket to Host:"+host.getHostName()+" Port:"+statusUpdatePort);
    	String localhost=null;
    	//establish socket connection to an agent where this process will report its state
        return new Socket(localhost, statusUpdatePort);

    }
    public void sendStateUpdate(String state) throws Exception {
    	DataOutputStream out = null;
    	Socket socket=null;
    	try {

    		StringBuilder sb =
            		new StringBuilder();
            
            sb.append("DUCC_PROCESS_UNIQUEID=").append(System.getenv("DUCC_PROCESS_UNIQUEID")).append(",");
            sb.append("DUCC_PROCESS_STATE=").append(state);
            socket = connectWithAgent();
            out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF(sb.toString());
            out.flush();
            System.out.println("Sent new State:"+state);
    	} catch( Exception e) {
    		throw e;
    	} finally {
    		if ( out != null ) {
        		out.close();
    		}
    		if ( socket != null ) {
    			socket.close();
    		}
    	}
      
    }
}
