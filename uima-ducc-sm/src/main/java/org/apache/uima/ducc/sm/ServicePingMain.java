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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.uima.ducc.cli.AServicePing;
import org.apache.uima.ducc.cli.ServiceStatistics;
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.utils.DuccProperties;


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

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	boolean debug = false;
    int error_max = 10;
    int error_count = 0;

    DuccProperties clioptions = new DuccProperties();

    public ServicePingMain()
    {
    	clioptions.put("--class", clioptions);
    	clioptions.put("--endpoint", clioptions);
    	clioptions.put("--port", clioptions);
    	clioptions.put("--arguments", clioptions);
    }

    static void usage()
    {

        System.out.println("Usage:");
        System.out.println("   java org.apache.uima.ducc.smnew.ServicePingMain <args>");
        System.out.println("Where args are:");
        System.out.println("   --class     classname       This is the class implementing the pinger.");
        System.out.println("   --endpoint  ep              This is the endpoint specified in the registration.");
        System.out.println("   --port      port            This is the listen port the SM is listening on.");
        System.out.println("   --arguments classname       These are the arguments for the pinger, supplied in the registration.");
        
        System.exit(1);
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
    AServicePing resolve(String cl, String args, String ep)
    {
    	print("ServicePingMain.resolve:", cl, "ep", ep);
    	AServicePing pinger = null;
		try {
			@SuppressWarnings("rawtypes")
			Class cls = Class.forName(cl);
			pinger = (AServicePing) cls.newInstance();
			pinger.init(args, ep);
		} catch (Exception e) {
            //print(e);         // To the logs
            e.printStackTrace();
		} 
        return pinger;
    }

    void handleError(AServicePing custom, Throwable t)
    {
        t.printStackTrace();
        if ( ++error_count >= error_max ) {
            custom.stop();
            System.out.println("Exceeded error count. Exiting.");
            System.exit(1);
        }
    }

    /**
     * Simple argument parser for this class.  It is spawned only by SM so even though we do
     * validity checking, we assume the args are correct and complete, and just crash hard if not as
     * it's an internal error that should not occur.
     */
    void parseOptions(String[] args)
    {
        // First read them all in
        if ( debug ) {
            for ( int i = 0; i < args.length; i++ ) {
                System.out.println("Args[" + i + "] = " + args[i]);
            }
        }

        for ( int i = 0; i < args.length; ) {
            if ( clioptions.containsKey(args[i]) ) {
                if ( clioptions.get(args[i]) != clioptions ) {
                    System.out.println("Duplicate argument, not allowed: " + args[i]);
                    System.exit(1);
                }
                System.out.println("Put " + args[i] + ", " + args[i+1]);
                clioptions.put(args[i], args[i+1]);
                i += 2;
            } else {
                System.out.println("Invalid argument: " + args[i]);
                System.exit(1);
            }
        }

        // Now make sure they all exist
        for ( Object o : clioptions.keySet() ) {
            String k = (String) o;
            if ( clioptions.get(k) == clioptions ) {
                System.out.println("Missing argument: " + k);
                System.exit(1);
            }
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
	protected int start(String[] args)
    {


        IServiceStatistics default_statistics = new ServiceStatistics(false, false, "<N/A>");

        parseOptions(args);
        String arguments = clioptions.getStringProperty("--arguments");
        String pingClass = clioptions.getStringProperty("--class");
        String endpoint  = clioptions.getStringProperty("--endpoint");
        int port         = clioptions.getIntProperty("--port");

        Socket sock = null;

		try {
			sock = new Socket("localhost", port);
		} catch (NumberFormatException e2) {
			e2.printStackTrace();
			return 1;
		} catch (UnknownHostException e2) {
			e2.printStackTrace();
			return 1;
		} catch (IOException e2) {
			e2.printStackTrace();
			return 1;
		} 

        print ("ServicePingMain listens on port", sock.getLocalPort());
        InputStream sock_in = null;
		OutputStream sock_out = null;
		try {
			sock_in = sock.getInputStream();
			sock_out = sock.getOutputStream();
		} catch (IOException e2) {
			e2.printStackTrace();
			return 1;
		}

        ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(sock_out);
			oos.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
			return 1;
		}        

        ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(sock_in);
		} catch (IOException e1) {
			e1.printStackTrace();
			return 1;
		}        

        AServicePing custom = resolve(pingClass, arguments, endpoint);
        if ( custom == null ) {
            print("bad_pinger:", pingClass, endpoint);
            return 1;
        }

        while ( true ) {  
        	if ( debug ) print("ServicePingMeta starts ping.");

            Ping ping = null;
			try {
                ping = (Ping) ois.readObject();
                if ( debug ) {
                    print("Total instances:" , ping.getSmState().getProperty("total-instances"));
                    print("Active instances:", ping.getSmState().getProperty("active-instances"));
                    print("References:"      , ping.getSmState().getProperty("references"));
                    print("Run Failures:"    , ping.getSmState().getProperty("runfailures"));
                }
			} catch (IOException e) {
                handleError(custom, e);
			} catch ( ClassNotFoundException e) {
				handleError(custom, e);
			}
            
            boolean quit = ping.isQuit();
            if ( debug ) print("Read ping: ", quit);

            try {
				if ( quit ) {
                    if ( debug ) System.out.println("Calling custom.stop");
				    custom.stop();                
                    oos.close();
                    ois.close();
                    sock.close();
                    if ( debug ) System.out.println("Custom.stop returns");
				    return 0;
                } else {
                    Pong pr = new Pong();
                    custom.setSmState(ping.getSmState());
                    IServiceStatistics ss = custom.getStatistics();
                    if ( ss == null ) {
                        ss = default_statistics;
                    }

                    pr.setStatistics     (ss);
                    pr.setAdditions      (custom.getAdditions());
                    pr.setDeletions      (custom.getDeletions());
                    pr.setExcessiveFailures(custom.isExcessiveFailures());


                    oos.writeObject(pr);
                    oos.flush();

                    // The ObjectOutputStream will cache instances and if all you do is change a
                    // field or two in the object, it won't be detected and the stale object will be
                    // sent.  So you have to reset() the stream, (or use a new object, or use
                    // clone() here also if you want, but this is simplest and safest since we have
                    // no control over what the external pinger gives us.
                    oos.reset();
				} 
            } catch (Throwable e) {
                handleError(custom, e);
			}            
        }
    }

    public static void main(String[] args)
    {
        ServicePingMain wrapper = new ServicePingMain();
        int rc = wrapper.start(args);
        System.exit(rc);
    }
    
}

