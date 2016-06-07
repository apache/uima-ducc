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
import org.apache.log4j.Level;
import org.apache.uima.ducc.common.admin.event.DuccAdminEvent;
import org.apache.uima.ducc.common.admin.event.RmAdminQLoad;
import org.apache.uima.ducc.common.admin.event.RmAdminQLoadReply;
import org.apache.uima.ducc.common.admin.event.RmAdminQOccupancy;
import org.apache.uima.ducc.common.admin.event.RmAdminQOccupancyReply;
import org.apache.uima.ducc.common.admin.event.RmAdminReconfigure;
import org.apache.uima.ducc.common.admin.event.RmAdminReply;
import org.apache.uima.ducc.common.admin.event.RmAdminVaryOff;
import org.apache.uima.ducc.common.admin.event.RmAdminVaryOn;
import org.apache.uima.ducc.common.admin.event.RmAdminVaryReply;
import org.apache.uima.ducc.common.authentication.BrokerCredentials;
import org.apache.uima.ducc.common.authentication.BrokerCredentials.Credentials;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.security.AnyTypePermission;

/**
 * This class provides the API and CLI to the Resource Managager administrative functions.
 *
 * The functions provided by this class are exposed both to Java programs, and scripting.  
 *
 * When invoked through the <code>main</code> class, the specified funtions emit a string which is
 * easily parsable by scripting.  Specifically, the qoccupancy and qload interfaces return a
 * string that can be converted to List and Dictionary objects in Python via Python's 
 * <code>eval</code> function.
 *
 * When invokded via Java the response is returned in Java objects, as described below.
 * The use of this class's Java API is intended for DUCC System programming, and requires
 * the Camel, Spring, ActiveMQ, and Log4j classes in the classpath, as well as the DUCC
 * Transport and Common jars.
 */ 
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

    /**
     * Creates a new instance of the RM administrative interface.

     * @param context This is the Camel context to use.  Usually it is sufficient to simply provide
     *                <code>new DefaultCamelContext()</code>.
     * @param epname  This is the RM JMS endpoint as configured in ducc.properties.  Usually it is
     *                sufficient to provide <code>ducc.rm.admin.endpoint</code>.
     */
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

    /**
     * This provides direct access to the logger for the class.
     */
    public DuccLogger getLogger()
    {
        DuccLogger ret = new DuccLogger("admin");
        ret.setLevel(Level.OFF);      // jrc UIMA-4358 disable logging for RM admin because
                                      // scripting has to scrape stdout and the log gets in the way
        return ret;
    }

    /**
     * Turn the request int xstream format for transmission to RM.
     */
    private String marshallEvent(DuccAdminEvent duccEvent) 
        throws Exception 
    {
        XStreamDataFormat xStreamDataFormat = new XStreamDataFormat();
        xStreamDataFormat.setPermissions("*");
        XStream xStream = xStreamDataFormat.getXStream(new DefaultClassResolver());
        xStream.addPermission(AnyTypePermission.ANY);
        
        return xStream.toXML(duccEvent);
    }
    
    /**
     * Marshall and transmit the request to RM, waiting for the response.
     * 
     * @param A DuccAdminEvent appropriate to the desired function.
     *
     * @return An {@link RmAdminReply RmAdminReply} appropriate to the response.  See the specific replies
     *         for details.

	 * @throws Exception if anything goes wrong in transmission or receipt of the request.
     */
    private RmAdminReply dispatchAndWaitForReply(DuccAdminEvent duccEvent) 
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
	 * This causes the RM to "vary off" a set of hosts.  The hosts may continue to broadcast state, and
     * RM trackes their online/offline status, but it stops scheduling to them.  If there is evictable work
     * on the hosts, RM will send eviction orders to the Orchestrator in order to clear them.
     *
     * @param args This is an array of hostnames indicating the hosts to be varied offline.
	 * 
     * @return A {@link RmAdminVaryReply RmAdminVaryReply} with success or failure status and if failure, the list of
     *         hosts that could not be varied off.
     *
	 * @throws Exception if anything goes wrong in transmission or receipt of the request.
	 */
	public RmAdminVaryReply varyoff(String[] args) 
		throws Exception 
    {
        String[] nodes = new String[args.length - 1];
        for ( int i = 1; i < args.length; i++) nodes[i-1] = args[i];  // take a slice of the array

        RmAdminVaryOff vo = new RmAdminVaryOff(nodes, user, cypheredMessage);
		return (RmAdminVaryReply) dispatchAndWaitForReply(vo);
	}

	/**
	 * This causes the RM to "vary on" a set of hosts. If the hosts are broadcasting state,
     * they are immediately available for scheduling.  This commnd does not start the DUCC agents,
     * it only instructs RM that, if the hosts was previously offline, it should now be used for
     * scheduling if and when the host is responding and sending heartbeats.
     *
     * @param args This is an array of hostnames indicating the hosts to be varied nline.
	 * 
     * @return A {@link RmAdminVaryReply RmAdminVaryReply} with success or failure status and if failure, the list of
     *         hosts that could not be varied on.
     *
	 * @throws Exception if anything goes wrong in transmission or receipt of the request.
	 */
	public RmAdminVaryReply varyon(String[] args) 
		throws Exception 
    {
        String[] nodes = new String[args.length - 1];
        for ( int i = 1; i < args.length; i++) nodes[i-1] = args[i];  // take a slice of the array

        RmAdminVaryOn vo = new RmAdminVaryOn(nodes, user, cypheredMessage);
		return (RmAdminVaryReply) dispatchAndWaitForReply(vo);
	}

	/**
	 * This queries the current workload demand and resource supply in RM.
     *
     * @return A {@link RmAdminQLoadReply RmAdminQLoadReply} containing data regarding the current 
     *         class and nodepool state.
	 * 
	 * @throws Exception if anything goes wrong in transmission or receipt of the request.
	 */
	public RmAdminQLoadReply qload()
		throws Exception 
    {
        RmAdminQLoad ql = new RmAdminQLoad(user, cypheredMessage);
		return (RmAdminQLoadReply) dispatchAndWaitForReply(ql);
	}

	/**
	 * This queries details on each host the RM is schedling to.
     *
     * @return A {@link RmAdminQOccupancyReply RmAdminQOccupancyReply} containing data regarding the current
     *         hosts.
	 * 
	 * @throws Exception if anything goes wrong in transmission or receipt of the request.
	 */
	public RmAdminQOccupancyReply qoccupancy()
		throws Exception 
    {
        RmAdminQOccupancy qo = new RmAdminQOccupancy(user, cypheredMessage);
		return (RmAdminQOccupancyReply) dispatchAndWaitForReply(qo);
	}
    
    // UIMA-4142
	/**
	 * Send a reconfigure event to RM.  RM rereads all its configuration data an possibly reconfigures
     * the schedule if needed.  
     *
     * @return {@link RmAdminReply RmAdminReply}.  The message must be <code>Reconfiguration complete.</code>;
     *         any other response indicates failure.  Failure occurs when the new configuration is invalid.  If this
     *         occurs use <code>check_ducc -cv </code> to read and validate the current configuration.
	 * 
	 * @throws Exception if anything goes wrong in transmission or receipt of the request.
	 */
	public RmAdminReply reconfigure()
		throws Exception 
    {
        RmAdminReconfigure np = new RmAdminReconfigure(user, cypheredMessage);
		return dispatchAndWaitForReply(np);
	}

    /**
     * This is called from <code>main</code> in response to a CLI request.
     *
     * @param args the command line arguments. See the usage method below for details.
     *
	 * @throws Exception if anything goes wrong in transmission or receipt of the request.
     */    
    public int run(String[] args)
    	throws Exception
    {
        // Construct the signature for the request
        user = System.getProperty("user.name");
        Crypto crypto = new Crypto(user, true);
        cypheredMessage = crypto.getSignature();
        
        if ( args[0].equals("--varyoff")) {
            if ( args.length < 2 ) usage("Missing node list");
            RmAdminVaryReply reply = varyoff(args);
            System.out.println(reply.getMessage());
            return (reply.getRc() ? 0 : 1);
        }

        if ( args[0].equals("--varyon")) {
            if ( args.length < 2 ) usage("Missing node list");
            RmAdminVaryReply reply = varyon(args);
            System.out.println(reply.getMessage());
            return (reply.getRc() ? 0 : 1);
        }

        if ( args[0].equals("--qload")) { 
            if ( args.length != 1 ) usage("Qload takes no arguments.");
            RmAdminQLoadReply reply = qload();
            System.out.println(reply.toString());
            return (reply.getRc() ? 0 : 1);
        }

        if ( args[0].equals("--qoccupancy")) {
            if ( args.length != 1 ) usage("Qoccupancy takes no arguments.");
            RmAdminQOccupancyReply reply = qoccupancy();
            System.out.println(reply.toString());
            return (reply.getRc() ? 0 : 1);
        }

        if ( args[0].equals("--reconfigure") ) {     // UIMA-4142
            if ( args.length != 1 ) usage("Reconfigure takes no arguments.");
            RmAdminReply reply = reconfigure();
            System.out.println(reply.getMessage());
            return (reply.getRc() ? 0 : 1);
        }

        System.out.println("Unknown command: " + args[0]);
        return 1;
    }

    private static void usage(String msg)
    {
        if ( msg != null ) System.out.println(msg);

        System.out.println("Usage:\n");
        System.out.println("DuccRmAdmin verb options");
        System.out.println("Where verbs are:");
        System.out.println("   --varyoff string-delimeted-nodes");
        System.out.println("   --varyon  string-delimeted-nodes");
        System.out.println("   --qload");
        System.out.println("   --qoccupancy");
        System.out.println("   --reconfigure");         // dynamic reconfig UIMA-4142

        System.exit(1);
    }

    /**
     * This is provided for use by the CLI, to invoke the varioius RM administrative commands.
     */
	public static void main(String[] args) 
    {
		int rc = 0;
		try {
			DuccRmAdmin admin = new DuccRmAdmin(new DefaultCamelContext(), "ducc.rm.admin.endpoint");
            rc = admin.run(args);
		} catch (Throwable e) {
			e.printStackTrace();
			rc = 1;
		} finally {
			System.exit(rc);
		}
	}
   
}
