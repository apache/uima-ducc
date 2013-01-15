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
package org.apache.uima.ducc.sm;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.uima.ducc.common.AServicePing;
import org.apache.uima.ducc.common.ServiceStatistics;


/**
 * If an external pinger is specified for a service, this method instantiates and executes
 * the pinger.
 *
 * The pinger must extend org.apache.uima.ducc.sm.cli.ServicePing and implement the ping() method.
 *
 */

public class ServicePingMain
    implements SmConstants
{

    boolean debug = false;
    int error_max = 10;
    int error_count = 0;

    public ServicePingMain()
    {
    	
    }

	@SuppressWarnings("static-access")
	private void addOptions(Options options) 
    {
        //
        // Verbs here
        //
		options.addOption(OptionBuilder
                          .withLongOpt    (ServicePing.Class.decode())
                          .withDescription(ServicePing.Class.description())
                          .withArgName    (ServicePing.Class.argname())
                          .hasOptionalArg ()
                          .create         ()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServicePing.Endpoint.decode())
                          .withDescription(ServicePing.Endpoint.description())
                          .withArgName    (ServicePing.Endpoint.argname())
                          .hasArg         (true)
                          .create         ()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServicePing.Port.decode())
                          .withDescription(ServicePing.Port.description())
                          .withArgName    (ServicePing.Port.argname())
                          .hasArg         (true)
                          .create         ()
                          );

    }

    static void appendStackTrace(StringBuffer s, Throwable t)
    {
    	s.append("\nAt:\n");
        StackTraceElement[] stacktrace = t.getStackTrace();
        for ( StackTraceElement ste : stacktrace ) {
            s.append("\t");
            s.append(ste.toString());
            s.append("\n");
        }
    }
    
    
    public static void print(Object ... args)
    {
    	StringBuffer s = new StringBuffer();
        for ( Object a : args ) {
            if ( a == null ) a = "<null>"; // avoid null pointers

            s.append(" ");
            if ( a instanceof Throwable ) {
            	Throwable t = (Throwable ) a;
                s.append(t.toString());
                s.append("\n");
                appendStackTrace(s, t);
            } else {                
                s.append(a.toString());
            }
        }
        System.err.println(s.toString());
    }

    //
    // resolve the customMeta string inta a class if we can
    //
    AServicePing resolve(String cl, String ep)
    {
    	print("ServicePingMain.resolve:", cl, "ep", ep);
    	AServicePing pinger = null;
		try {
			@SuppressWarnings("rawtypes")
			Class cls = Class.forName(cl);
			pinger = (AServicePing) cls.newInstance();
			pinger.init(ep);
		} catch (Exception e) {
            //print(e);         // To the logs
            e.printStackTrace();
		} 
        return pinger;
    }

    void handleError(Throwable t)
    {
        t.printStackTrace();
        if ( ++error_count >= error_max ) {
            System.out.println("Exceeded error count. Exiting.");
            System.exit(1);
        }
    }

    //
    // 1. Instantiate the pinger if possible.
    // 2. Read ducc.proeprties to find the ping interval
    // 3. Start pinging and wriging results to stdout
    //
    // The ServiceManager must start this process as the user.  It monitors stdout for success
    // or failute of the ping and reacts accordingly.
    //
    protected void start(String[] args)
    {


        Options options = new Options();
        addOptions(options);

        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = null;
        ServiceStatistics default_statistics = new ServiceStatistics(false, false, "<N/A>");

		try {
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
            print("Cannot parse command line:", e);
            return;
		}

        String pingClass = commandLine.getOptionValue(ServicePing.Class.decode());
        String endpoint  = commandLine.getOptionValue(ServicePing.Endpoint.decode());
        String port      = commandLine.getOptionValue(ServicePing.Port.decode());

        Socket sock = null;
		try {
			sock = new Socket("localhost", Integer.parseInt(port));
		} catch (NumberFormatException e2) {
			e2.printStackTrace();
			return;
		} catch (UnknownHostException e2) {
			e2.printStackTrace();
			return;
		} catch (IOException e2) {
			e2.printStackTrace();
			return;
		}        
        print ("ServicePingMain listens on port", sock.getLocalPort());
        InputStream sock_in = null;
		OutputStream sock_out = null;
		try {
			sock_in = sock.getInputStream();
			sock_out = sock.getOutputStream();
		} catch (IOException e2) {
			e2.printStackTrace();
			return;
		}

        ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(sock_out);
			oos.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}        

        AServicePing custom = resolve(pingClass, endpoint);
        if ( custom == null ) {
            print("bad_pinger:", pingClass, endpoint);
            return;
        }

        while ( true ) {  
        	if ( debug ) print("ServicePingMeta starts ping.");
        	
            byte[] cmd = new byte[1];
            cmd[0] = 0;
            int eof = 0;
			try {
				eof = sock_in.read(cmd);
			} catch (IOException e) {
                handleError(e);
			}
            if ( debug ) print("Read cmd", new String(cmd), "eof", eof);

            if ( eof == -1 ) {
                print("EOF on input pipe.  Exiting");
                custom.stop();
                return;
            }

            try {
				if ( cmd[0] == 'P' ) {
                    ServiceStatistics ss = custom.getStatistics();
                    if ( ss == null ) {
                        ss = default_statistics;
                    }
                    // print("Is alive: " + ss.isAlive());
                    oos.writeObject(ss);
                    oos.flush();

                    // The ObjectOutputStream will cache instances and if all you do is change a
                    // field or two in the object, it won't be detected and the stale object will be
                    // sent.  So you have to reset() the stream, (or use a new object, or use
                    // clone() here also if you want, but this is simplest and safest since we have
                    // no control over what the external pinger gives us.
                    oos.reset();
				} else if ( cmd[0] == 'Q' ) {
				    custom.stop();                
				    return;
				} else {
				    System.err.println("Invalid command recieved: " +  Byte.toString(cmd[0]));
				}
			} catch (Throwable e) {
                handleError(e);
			}            
        }
    }

    public static void main(String[] args)
    {
        ServicePingMain wrapper = new ServicePingMain();
        wrapper.start(args);
    }
    
}
