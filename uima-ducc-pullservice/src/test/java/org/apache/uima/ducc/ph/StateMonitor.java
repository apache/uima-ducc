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
package org.apache.uima.ducc.ph;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;


public class StateMonitor {
	private int port;
	private ServerSocket serviceStateUpdateServer;
	private Thread serverThread;
	private boolean running = false;
	public void stop() throws Exception {
		running = false;
		serviceStateUpdateServer.close();
	}
    public void start() throws Exception {
		  	serviceStateUpdateServer = new ServerSocket(0);
		  	serviceStateUpdateServer.setReuseAddress(true);
            port = serviceStateUpdateServer.getLocalPort();
		  	// Publish State Update Port for AP's. This port will be added to the AP
		  	// environment before a launch
            System.setProperty("DUCC_PROCESS_UNIQUEID", "1234");
            System.setProperty("DUCC_STATE_UPDATE_PORT", String.valueOf(port));
		  	// spin a server thread which will handle AP state update messages
		  	running = true;
		  	serverThread = new Thread( new Runnable() {
		  		public void run() {
		  			while(running) {
		  		  		try {
		  		  			Socket client = serviceStateUpdateServer.accept();
		  		  			// AP connected, get its status report. Handling of the status
		  		  			// will be done in a dedicated thread to allow concurrent processing.
		  		  			// When handling of the state update is done, the socket will be closed
		  		  			ServiceUpdateWorkerThread worker = new ServiceUpdateWorkerThread(client);
		  		  			worker.start();
		  		  		} catch( SocketException e) {
		  		  			
		  		  			break;
		  		  		} catch( Exception e) {
		  		  			e.printStackTrace();
		  		  			break;
		  		  		} finally {
		  		  		}
		  		  		
		  		  	}	
		  			System.out.println("Monitor thread stopped");;
		  		}
		  	});
		  	serverThread.start();
		  	System.out.println("Started Monitor on Port"+port);
		  }
	
	
	  class ServiceUpdateWorkerThread extends Thread {
		  private Socket socket;
		  ServiceUpdateWorkerThread(Socket socket) {
			  this.socket = socket;
		  }
		  
		  public void run() {
			  try {
				  DataInputStream dis = new DataInputStream(socket.getInputStream());
				  String state = dis.readUTF();
				  //updateHandler.onProcessStateUpdate(state);
				  System.out.println(">>>>> Monitor Received State Update:"+state);
			  } catch( Exception e) {
				  e.printStackTrace();
			  } finally {
				  try {
					  socket.close();
				  } catch(Exception e) {
					  e.printStackTrace();
				  }
			  }
		  }
	  }
}
