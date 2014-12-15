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
package org.apache.uima.ducc.common.main;

import java.io.FileNotFoundException;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RuntimeExchangeException;
import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultClassResolver;
import org.apache.uima.ducc.common.admin.event.DuccAdminEvent;
import org.apache.uima.ducc.common.admin.event.RmAdminQLoad;
import org.apache.uima.ducc.common.admin.event.RmAdminQLoadReply;
import org.apache.uima.ducc.common.admin.event.RmAdminQOccupancy;
import org.apache.uima.ducc.common.admin.event.RmAdminQOccupancyReply;
import org.apache.uima.ducc.common.admin.event.RmAdminReconfigure;
import org.apache.uima.ducc.common.admin.event.RmAdminReply;
import org.apache.uima.ducc.common.admin.event.RmAdminVaryOff;
import org.apache.uima.ducc.common.admin.event.RmAdminVaryOn;
import org.apache.uima.ducc.common.authentication.BrokerCredentials;
import org.apache.uima.ducc.common.authentication.BrokerCredentials.Credentials;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;

import com.thoughtworks.xstream.XStream;

public class DuccRmAdmin 
    extends AbstractDuccComponent 
{
	public static final String FileSeparator = System
        .getProperty("file.separator");

	private String brokerUrl;
	private ProducerTemplate pt;
	private String targetEndpoint;

    String user;
    byte[] cypheredMessage;

	public DuccRmAdmin(CamelContext context, String epname)
    {
		super("DuccServiceReaper", context);
		try {
                        
			// Load ducc properties file and enrich System properties. It supports
			// overrides for entries in ducc properties file. Any key in the ducc
			// property file can be overriden with -D<key>=<value>
			loadProperties(DuccService.DUCC_PROPERTY_FILE);

			// fetch the broker URL from ducc.properties
			this.brokerUrl = System.getProperty("ducc.broker.url");
			try {
				String brokerCredentialsFile = System.getProperty("ducc.broker.credentials.file");
				// fetch the admin endpoint from the ducc.properties where
				// the admin events will be sent by the DuccServiceReaper
				targetEndpoint = System.getProperty(epname);
                if ( targetEndpoint == null ) {
                    throw new IllegalArgumentException("Cannot find endpoint for RM admin.  Is 'ducc.rm.admin.endpoint' configured n ducc.properties?");
                }

				// System.out.println("+++ Activating JMS Component for Endpoint:" + targetEndpoint + " Broker:" + brokerUrl);
				
				ActiveMQComponent duccAMQComponent = new ActiveMQComponent(context);
			    duccAMQComponent.setBrokerURL(brokerUrl);
				
                //				context.addComponent("activemq",
                //						ActiveMQComponent.activeMQComponent(brokerUrl));
			    
			    if ( brokerCredentialsFile != null && brokerCredentialsFile.length() > 0 ) {
			    	String path ="";
			    	try {
                        Utils.findDuccHome();  // add DUCC_HOME to System.properties
                        path = Utils.resolvePlaceholderIfExists(brokerCredentialsFile, System.getProperties());
                        Credentials credentials = BrokerCredentials.get(path);
                        if ( credentials.getUsername() != null && credentials.getPassword() != null ) {
							duccAMQComponent.setUserName(credentials.getUsername());
							duccAMQComponent.setPassword(credentials.getPassword());
                        }   
			    	} catch(FileNotFoundException e) {
						System.out.println("DuccRmAdmin Failed - File Not Found:"+path+" Broker Credentials File:"+brokerCredentialsFile);
						System.exit(-1);
			    	}
			    }
				context.addComponent("activemq",duccAMQComponent);
				this.pt = context.createProducerTemplate();
			} catch( Throwable exx) {
				System.out.println("DuccRmAdmin Failed:"+exx);
				System.exit(-1);
			}

		} catch (Exception e) {
            e.printStackTrace();
			System.exit(-1);
		}
	}

    public DuccLogger getLogger()
    {
        return new DuccLogger("Admin");
    }

    private String marshallEvent(DuccAdminEvent duccEvent) 
        throws Exception 
    {
        XStreamDataFormat xStreamDataFormat = new XStreamDataFormat();
        XStream xStream = xStreamDataFormat.getXStream(new DefaultClassResolver());
        return xStream.toXML(duccEvent);
    }
    
    // private RmAdminReply unmarshallEvent(Object targetToUnmarshall) 
    //     throws Exception 
    // {
    //     XStream xStream = new XStream(new DomDriver());
    //     String claz = targetToUnmarshall.getClass().getName();

    //     if (targetToUnmarshall instanceof byte[]) {
    //         Object reply = xStream.fromXML(new String((byte[]) targetToUnmarshall));
    //         if (reply instanceof RmAdminReply) {
    //             return (RmAdminReply) reply;
    //         } else {
    //             claz = (reply == null) ? "NULL" : reply.getClass().getName();
    //         }
    //     }
    //     throw new Exception( "Unexpected Reply type received from Ducc Component. Expected DuccEvent, instead received: " + claz);        
    // }

    public RmAdminReply dispatchAndWaitForReply(DuccAdminEvent duccEvent) 
        throws Exception 
    {
        int maxRetryCount = 20;
        int i = 0;
        Object reply = null;
        RuntimeExchangeException ree = null;

        // retry up to 20 times. This is an attempt to handle an error thrown
        // by Camel: Failed to resolve replyTo destination on the exchange
        // Camel waits at most 10000ms( 10secs) for AMQ to create a temp queue.
        // After 10secs Camel times out and throws an Exception.
        for (; i < maxRetryCount; i++) {
            try {
                reply = pt.sendBody(targetEndpoint, ExchangePattern.InOut, marshallEvent(duccEvent));
                ree = null; // all is well - got a reply
                break; // done here

            } catch (RuntimeExchangeException e) {
                String msg = e.getMessage();
                // Only retry if AMQ failed to create a temp queue
                if (msg != null && msg.startsWith("Failed to resolve replyTo destination on the exchange")) {
                    ree = e;
                } else {
                    throw new DuccRuntimeException("Ducc JMS Dispatcher is unable to deliver a request.", e);
                }
            }
        }
        // when retries hit the threshold, just throw an exception
        if (i == maxRetryCount) {
            throw new DuccRuntimeException("ActiveMQ failed to create temp reply queue. After 20 attempts to deliver request to the OR, Ducc JMS Dispatcher is giving up.",
                                           ree);
        }
        if ( reply instanceof RmAdminReply ) {
            return (RmAdminReply) reply;
        } else {
            throw new DuccRuntimeException("Received unexpected object as response: " + reply.getClass().getName());
        }
    }

	/**
	 * Interprets and executes Admin command
	 * 
	 * @throws Exception
	 */
	public void varyoff(String[] args) 
		throws Exception 
    {
        String[] nodes = new String[args.length - 1];
        for ( int i = 1; i < args.length; i++) nodes[i-1] = args[i];  // take a slice of the array

        RmAdminVaryOff vo = new RmAdminVaryOff(nodes, user, cypheredMessage);
		RmAdminReply reply = dispatchAndWaitForReply(vo);
		System.out.println(reply.getResponse());
	}

	/**
	 * Interprets and executes Admin command
	 * 
	 * @throws Exception
	 */
	public void varyon(String[] args) 
		throws Exception 
    {
        String[] nodes = new String[args.length - 1];
        for ( int i = 1; i < args.length; i++) nodes[i-1] = args[i];  // take a slice of the array

        RmAdminVaryOn vo = new RmAdminVaryOn(nodes, user, cypheredMessage);
		RmAdminReply reply = dispatchAndWaitForReply(vo);
		System.out.println(reply.getResponse());
	}

	/**
	 * Query load.
	 * 
	 * @throws Exception
	 */
	public RmAdminQLoadReply qload()
		throws Exception 
    {
        RmAdminQLoad ql = new RmAdminQLoad(user, cypheredMessage);
		return (RmAdminQLoadReply) dispatchAndWaitForReply(ql);
	}

	/**
	 * Query occupancy.
	 * 
	 * @throws Exception
	 */
	public RmAdminQOccupancyReply qoccupancy()
		throws Exception 
    {
        RmAdminQOccupancy qo = new RmAdminQOccupancy(user, cypheredMessage);
		return (RmAdminQOccupancyReply) dispatchAndWaitForReply(qo);
	}
    
	/**
	 * Send reconfigure event to RM.
     * UIMA-4142
	 * 
	 * @throws Exception
	 */
	public void reconfigure()
		throws Exception 
    {
        RmAdminReconfigure np = new RmAdminReconfigure(user, cypheredMessage);
		RmAdminReply reply = dispatchAndWaitForReply(np);
		System.out.println(reply.getResponse());
	}

    
    public void run(String[] args)
    	throws Exception
    {

        user = System.getProperty("user.name");
    	Crypto crypto = new Crypto(user,System.getProperty("user.home"));
        cypheredMessage = crypto.encrypt(user);

        if ( args[0].equals("--varyoff")) {
            if ( args.length < 2 ) usage("Missing node list");
            varyoff(args);
            return;
        }

        if ( args[0].equals("--varyon")) {
            if ( args.length < 2 ) usage("Missing node list");
            varyon(args);
            return;
        }

        if ( args[0].equals("--qload")) { 
            if ( args.length != 2 ) usage("Query load: specify --console or --compact");
            if ( !args[1].equals("--console") && !args[1].equals("--compact") ) {
                usage("Invalid argument: " + args[1] + " - specify --console or --compact");
            }

            RmAdminQLoadReply ret = qload();
            if ( args[1].equals("--console") ) {
                System.out.println(ret.toConsole());
            } else {
                System.out.println(ret.toCompact());
            }
            return;
        }

        if ( args[0].equals("--qoccupancy")) {
            if ( args.length != 2 ) usage("Query occupancy: specify --console or --compact");
            if ( !args[1].equals("--console") && !args[1].equals("--compact") ) {
                usage("Invalid argument: " + args[1] + " - specify --console or --compact");
            }
            RmAdminQOccupancyReply ret = qoccupancy();
            if ( args[1].equals("--console") ) {
                System.out.println(ret.toConsole());
            } else {
                System.out.println(ret.toCompact());
            }

            return;
        }

        if ( args[0].equals("--reconfigure") ) {     // UIMA-4142
            if ( args.length != 1 ) usage("Reconfigure takes no arguments.");
            reconfigure();
            return;
        }

        System.out.println("Unknown command: " + args[0]);
    }

    public static void usage(String msg)
    {
        if ( msg != null ) System.out.println(msg);

        System.out.println("Usage:\n");
        System.out.println("DuccRmAdmin verb options");
        System.out.println("Where verbs are:");
        System.out.println("   --varyoff string-delimeted-nodes");
        System.out.println("   --varyon  string-delimeted-nodes");
        System.out.println("   --qload --console|--compact");
        System.out.println("   --qoccupancy --console|--compact");
        System.out.println("   --reconfigure");         // dynamic reconfig UIMA-4142

        System.exit(1);
    }

	public static void main(String[] args) 
    {
		try {
			DuccRmAdmin admin = new DuccRmAdmin(new DefaultCamelContext(), "ducc.rm.admin.endpoint");
            admin.run(args);
		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			System.exit(-1);
		}
	}
   
}
