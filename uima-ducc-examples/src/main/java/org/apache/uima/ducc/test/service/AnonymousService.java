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
package org.apache.uima.ducc.test.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;


public class AnonymousService
{
    String sockdir;
    AnonymousService(String sockdir)
        throws Exception
    {
        System.out.println("Custom Service starts");
        this.sockdir = sockdir;
        File f = new File(sockdir);
        if ( f.exists() && f.isDirectory() ) {
            System.out.println("Reusing socket dir: " + sockdir);
            return;
        }

        if ( f.exists()) {
            throw new IllegalStateException("Socket tempdir conflict, exits but is not a directory: " + sockdir);
        }
        
        System.out.println("Making new sockdir: " + sockdir);
        f.mkdirs();
    }

    void startPingResponder()
    	throws Exception
    {
        PingResponder pr = new PingResponder(sockdir);
        Thread prth = new Thread(pr);
        prth.start();
    }

    void run()
    	throws Exception
    {
        startPingResponder();

        try {
			while ( true ) {
			    System.out.println("Serving stuff");
			    Thread.sleep(30000);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public static void main(String[] args)
    {
        if ( args.length > 0 ) {
            for ( int i = 0; i < args.length; i++ ) {
                System.out.println("Args[" + i + "] = " + args[i]);
            }
        } else {
            System.out.println("No args, can't run");
            System.exit(1);
        }

        try {
			AnonymousService as = new AnonymousService(args[0]);
			as.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    class PingResponder
        implements Runnable
    {
        String sockdir;
        String hostname;
        PingResponder(String sockdir)
        	throws Exception
        {
            this.sockdir = sockdir;
            this. hostname = InetAddress.getLocalHost().getCanonicalHostName();
            System.out.println("Starting ping responder on " + hostname);
        }

        public void run()
        {
            ServerSocket server = null;
            System.out.println("Waiting for ping");
            int pingid = 0;
            try {

                server = new ServerSocket(0);
                int port = server.getLocalPort();

                System.out.println("ServicePingMain listens on port " + port);
                
                File f = new File(sockdir + "/hostport");
                FileOutputStream fos = new FileOutputStream(f);
                fos.write((hostname + ":" + port).getBytes());
                fos.close();

                while ( true ) {
                    Socket sock = server.accept();
                    ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
                    String reply = "Pong number " + pingid++;
                    System.out.println("Responding to ping: " + reply);
                    out.writeObject(reply);
                    out.flush();
                    out.close();                    
                }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
                return;
			} finally {
                try {
					server.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
            }
        }
    }
}
