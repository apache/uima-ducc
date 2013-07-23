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
import java.util.Properties;

import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;


/**
 * Submit a DUCC reservation
 */

public class DuccReservationSubmit 
    extends CliBase
{
    static String or_port = "ducc.orchestrator.http.port";
    static String or_host = "ducc.orchestrator.http.node";

    ReservationRequestProperties requestProperties = new ReservationRequestProperties();
	
	private String nodeList = "";
	
    /**
     * @param args Array of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationSubmit(String[] args)
        throws Exception
    {
        init(this.getClass().getName(), opts, args, requestProperties, or_host, or_port, "or");
    }

    /**
     * @param args List of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationSubmit(ArrayList<String> args)
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        init(this.getClass().getName(), opts, arg_array, requestProperties, or_host, or_port, "or");
    }

    /**
     * @param props Properties file of arguments, as described in the
     *      <a href="/doc/duccbook.html#DUCC_CLI_RESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationSubmit(Properties props)
        throws Exception
    {
        for ( Object k : props.keySet() ) {      
            Object v = props.get(k);
            requestProperties.put(k, v);
        }
        init(this.getClass().getName(), opts, null, requestProperties, or_host, or_port, "or");
    }

    UiOption[] opts = new UiOption[] {
        UiOption.Help,
        UiOption.Debug, 

        UiOption.Description,
        UiOption.SchedulingClass,
        UiOption.NumberOfInstances,
        UiOption.Specification,
        UiOption.ReservationMemorySize,
    };

// 	@SuppressWarnings("static-access")
// 	private void addOptions(Options options) {
// // // 		options.addOption(OptionBuilder
// // // 				.withDescription(DuccUiConstants.desc_help).hasArg(false)
// // // 				.withLongOpt(DuccUiConstants.name_help).create());
// // 		options.addOption(OptionBuilder
// // 				.withArgName(DuccUiConstants.parm_description)
// // 				.withDescription(makeDesc(DuccUiConstants.desc_description,DuccUiConstants.exmp_description)).hasArg()
// // 				.withLongOpt(DuccUiConstants.name_description).create());
// 		options.addOption(OptionBuilder
// 				.withArgName(DuccUiConstants.parm_reservation_scheduling_class)
// 				.withDescription(makeDesc(DuccUiConstants.desc_reservation_scheduling_class,DuccUiConstants.exmp_reservation_scheduling_class)).hasArg()
// 				.withLongOpt(DuccUiConstants.name_reservation_scheduling_class).create());
// 		options.addOption(OptionBuilder
// 				.withArgName(DuccUiConstants.parm_number_of_instances)
// 				.withDescription(makeDesc(DuccUiConstants.desc_number_of_instances,DuccUiConstants.exmp_number_of_instances)).hasArg()
// 				.withLongOpt(DuccUiConstants.name_number_of_instances).create());
// 		options.addOption(OptionBuilder
// 				.withArgName(DuccUiConstants.parm_instance_memory_size)
// 				.withDescription(makeDesc(DuccUiConstants.desc_instance_memory_size,DuccUiConstants.exmp_instance_memory_size)).hasArg()
// 				.withLongOpt(DuccUiConstants.name_instance_memory_size).create());
// 		options.addOption(OptionBuilder
// 				.withArgName(DuccUiConstants.parm_specification)
// 				.withDescription(DuccUiConstants.desc_specification).hasArg()
// 				.withLongOpt(DuccUiConstants.name_specification).create());
// 	}
	

    /**
     * Execute collects the parameters for the reservation and sends them to the DUCC Orchestrator
     * to schedule the reservation.  This method blocks until either the reservation is 
     * granted, or it fails.  Failure is always do to lack of resources, in some form or another.
     * Reservations are granted all-or-nothing: you get everything you ask for, or you get nothing.
     *
     * @return True if the DUCC grants the reservation. 
     */
	public boolean execute()
    {		
        if ( ! requestProperties.containsKey(UiOption.NumberOfInstances.pname()) ) {
            requestProperties.put(UiOption.NumberOfInstances.pname(), UiOption.NumberOfInstances.deflt());
        }
     
        SubmitReservationDuccEvent      ev    = new SubmitReservationDuccEvent(requestProperties);
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

        if ( rc ) { 
        	nodeList = reply.getProperties().getProperty(UiOption.ReservationNodeList.pname());
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
                
                // Fetch the Ducc ID
            	System.out.println("Process " + ds.getDuccId() + " submitted.");
                System.out.println("resID = " + ds.getDuccId());
                System.out.println("nodes: "  + ds.getHostsAsString());
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
