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
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;


/**
 * Submit a DUCC reservation
 */

public class DuccReservationSubmit 
    extends CliBase
{
    ReservationRequestProperties requestProperties = new ReservationRequestProperties();
    IDuccCallback resCB = new ReservationCallback();
	
	private String nodeList = "";
	
    /**
     * @param args Array of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationSubmit(String[] args)
        throws Exception
    {
        init (this.getClass().getName(), opts, args, requestProperties, resCB);
    }

    /**
     * @param args List of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationSubmit(ArrayList<String> args)
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        init (this.getClass().getName(), opts, arg_array, requestProperties, resCB);
    }

    /**
     * @param props Properties file of arguments, as described in the
     *      <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationSubmit(Properties props)
        throws Exception
    {
        init (this.getClass().getName(), opts, props, requestProperties, resCB);
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
        
        // If request was accepted, default to starting a monitor so can report the state 
        // but only if the user has not specified this option
        if (rc) {
        	if (!commandLine.contains(UiOption.WaitForCompletion)) {
        		requestProperties.setProperty(UiOption.WaitForCompletion.pname(), "");
        	} 
            startMonitors(false, DuccContext.Reservation);       // starts conditionally, based on job spec and console listener present
        }
        
        // Note: in DUCC 1.x this waited for a response.  With 2.0 requests are queued and the caller can 
        // choose to not wait, although that is the default.
        // Now like the other requests, the wait is done when getting the rc
        return rc;
    }

    /**
     * If the reservation is granted, this method returns the reserved host
     * @return Name of host where the reservation is granted.
     */
    public String getHost()
    {
        return nodeList;
    }
	
	/**
     * If the reservation is granted, this method returns the set of hosts containing the reservation.
     * @return String array of hosts where the reservation is granted.
     */
	@Deprecated
	public String[] getHosts() 
    {
		  return this.nodeList.split("\\s");
	}
	
    /**
     * If the reservation is granted, this method returns the set of hosts containing the reservation as
     * a single blank-delimited string.
     * @return Blank-delimited string of hosts where the reservation is granted.
     */
	@Deprecated
    public String getHostsAsString()
    {
        return nodeList;
    }

    /**
     * Main method, as used by the executable jar or direct java invocation.
     * @param args arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public static void main(String[] args) {
		int code = 1;
        try {
            // Instantiate the object with args similar to the CLI, or a pre-built properties file
            DuccReservationSubmit ds = new DuccReservationSubmit(args);

            // Run the API.  If process_attach_console was specified in the args, a console listener is
            // started but this call does NOT block on it.
            boolean rc = ds.execute();

            // If the return is 'true' then as best the API can tell, the submit worked
            if ( rc ) {
                System.out.println("Reservation "+ds.getDuccId()+" submitted.");
                code = ds.getReturnCode();
                // Note:  code is not (but should be) non-zero when request fails
                String node = ds.getHost();
                if (!node.isEmpty()) {
                	System.out.println("Node: " + node);
                }
            } else {
                System.out.println("Could not submit reservation.");
            }
        } catch (Exception e) {
            System.out.println("Cannot initialize: " + e.getMessage() + ".");
        } finally {
            // Set the process exit code
            System.exit(code);
        }
	}
	
	/*
	 * Special callback that extracts the assigned node
	 */
	private class ReservationCallback implements IDuccCallback {

		private final String nodesPrefix = "nodes: ";
		
		public void console(int pnum, String msg) {
			System.out.println("[" + pnum + "] " + msg);
		}

		public void status(String msg) {
			if (msg.startsWith(nodesPrefix)) {
				nodeList = msg.substring(nodesPrefix.length());
			} else {
				System.out.println(msg);
			}
		}

	}
}
