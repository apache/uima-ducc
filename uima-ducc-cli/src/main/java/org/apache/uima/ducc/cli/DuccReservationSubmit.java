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
package org.apache.uima.ducc.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.ReservationReplyProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;


/**
 * Submit a DUCC reservation
 */

public class DuccReservationSubmit 
    extends CliBase
{
    ReservationRequestProperties requestProperties = new ReservationRequestProperties();
	
	private String nodeList = "";
	private String info = "";
	
    /**
     * @param args Array of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationSubmit(String[] args)
        throws Exception
    {
        init (this.getClass().getName(), opts, args, requestProperties, null);
    }

    /**
     * @param args List of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationSubmit(ArrayList<String> args)
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        init (this.getClass().getName(), opts, arg_array, requestProperties, null);
    }

    /**
     * @param props Properties file of arguments, as described in the
     *      <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationSubmit(Properties props)
        throws Exception
    {
        init (this.getClass().getName(), opts, props, requestProperties, null);
    }

    UiOption[] opts = new UiOption[] {
        UiOption.Help,
        UiOption.Debug, 

        UiOption.Description,
        UiOption.SchedulingClass,
        UiOption.Specification,
        UiOption.ReservationMemorySize,
        UiOption.Timestamp,
        UiOption.WaitForCompletion,
        UiOption.CancelOnInterrupt,
    };


    /**
     * Execute collects the parameters for the reservation and sends them to the DUCC Orchestrator
     * to schedule the reservation.  This method blocks until either the reservation is 
     * granted, or it fails.  Failure is always do to lack of resources, in some form or another.
     * Reservations must be from one of the 'reserve' classes i.e for a whole machine.
     *
     * @return True if the DUCC grants the reservation. 
     * @throws Exception 
     */
	public boolean execute() throws Exception
    {		
        String pname = UiOption.SchedulingClass.pname();
        String scheduling_class = requestProperties.getProperty(pname);
        if (scheduling_class != null) {
            String[] reserveClasses = DuccSchedulerClasses.getInstance().getReserveClasses();
            if (!Arrays.asList(reserveClasses).contains(scheduling_class)) {
            	throw new IllegalArgumentException("Invalid value for scheduling_class - must be one of the reserve classes");
            }
        }
        SubmitReservationDuccEvent      ev    = new SubmitReservationDuccEvent(requestProperties, CliVersion.getVersion());
        SubmitReservationReplyDuccEvent reply = null;
        
        try {
            reply = (SubmitReservationReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            message("Reservation not submitted:", e.getMessage());
            return false;
        } finally {
            dispatcher.close();
        }

        /*
         * process reply (gets friendlyId and messages.
         */
        boolean rc = extractReply(reply);

        if ( !rc ) { 
			return false;
		}
		
        System.out.println("Reservation " + getDuccId() + " submitted.");
        System.out.println("resID = " + getDuccId() + " "+info);
        
        // If request was accepted, always start a monitor so can report the state 
        requestProperties.setProperty(UiOption.WaitForCompletion.pname(), "true");
		startMonitors(false, DuccContext.Reservation);       // starts conditionally, based on job spec and console listener present
		
		// Then since this is a synchronous api, wait for request to complete, as only then will the node be available
		rc = (getReturnCode() == 0);
		if (rc) {
        	nodeList = reply.getProperties().getProperty(UiOption.ReservationNodeList.pname());
        }
        
        if((nodeList.trim()).length() == 0) {
        	Properties properties = reply.getProperties();
            if(properties != null) {
            	String key = ReservationReplyProperties.key_message;
            	if(properties.containsKey(key)) {
            		info = properties.getProperty(key);
            	}
            }
        }
        
        return rc;
    }

	/**
     * If the reservation is granted, this method returns the set of hosts containing the reservation.
     * @return String array of hosts where the reservation is granted.
     */
	public String[] getHosts() 
    {
		  return this.nodeList.split("\\s");
	}
	
    /**
     * If the reservation is granted, this method returns the set of hosts containing the reservation as
     * a single blank-delimeted string.
     * @return Blank-delimeted string of hosts where the reservation is granted.
     */
    public String getHostsAsString()
    {
        return nodeList;
    }

    /**
     * Main method, as used by the executable jar or direct java invocation.
     * @param args arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public static void main(String[] args) {
        try {
            // Instantiate the object with args similar to the CLI, or a pre-built properties file
            DuccReservationSubmit ds = new DuccReservationSubmit(args);

            // Run the API.  If process_attach_console was specified in the args, a console listener is
            // started but this call does NOT block on it.
            boolean rc = ds.execute();

            // If the return is 'true' then as best the API can tell, the submit worked
            if ( rc ) {
            	System.exit(0);
            } else {
                System.out.println("Could not submit reservation.");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("Cannot initialize: " + e.getMessage() + ".");
            System.exit(1);
        }
	}
	
}
