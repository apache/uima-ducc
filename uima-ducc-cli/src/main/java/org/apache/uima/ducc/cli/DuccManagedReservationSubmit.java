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

import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;


/**
 * Submit a DUCC Managed Reservation.  A Managed Reservation is a single arbitray process running
 * in a non-preemptable share.
 */

public class DuccManagedReservationSubmit 
    extends CliBase
{
    private static String dt = "Managed Reservation";
    
    private ServiceRequestProperties serviceRequestProperties;

    private UiOption[] opts = new UiOption[] {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Description,
        UiOption.Environment,
        UiOption.LogDirectory,
        UiOption.AttachConsole,
        UiOption.ProcessExecutableRequired,
        UiOption.ProcessExecutableArgs,
        UiOption.ProcessMemorySize,
        UiOption.SchedulingClass,
        UiOption.Specification,
        UiOption.SuppressConsoleLog,
        UiOption.WorkingDirectory,
        UiOption.WaitForCompletion,
        UiOption.CancelOnInterrupt,
        UiOption.ServiceDependency,
    };

    /**
     * @param args List of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_PROCESS_SUBMIT">DUCC CLI reference.</a>
     */
    public DuccManagedReservationSubmit(String[] args)
        throws Exception
    {
        this(args, null);
    }
        
    /**
     * @param args Array of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_PROCESS_SUBMIT">DUCC CLI reference.</a>
     */
    public DuccManagedReservationSubmit(ArrayList<String> args)
        throws Exception
    {
        this(args, null);
    }
        
    /**
     * @param props Properties file of arguments, as described in the
     *      <a href="/doc/duccbook.html#DUCC_CLI_PROCESS_SUBMIT">DUCC CLI reference.</a>
     */ 
   public DuccManagedReservationSubmit(Properties props)
        throws Exception
    {
        this(props, null);
    }

    /**
     * This form of the constructor allows the API user to capture
     * messages, rather than directing them to stdout. 
     *
     * @param args Array of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_PROCESS_SUBMIT">DUCC CLI reference.</a>
     * @param consoleCb If provided, messages are directed to it instead of
     *        stdout.
     */
    public DuccManagedReservationSubmit(String[] args, IDuccCallback consoleCb)
        throws Exception
    {
        serviceRequestProperties = new ServiceRequestProperties(); 
        init (this.getClass().getName(), opts, args, serviceRequestProperties, consoleCb);
    }
        
    /**
     * This form of the constructor allows the API user to capture
     * messages, rather than directing them to stdout. 
     *
     * @param args List of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_PROCESS_SUBMIT">DUCC CLI reference.</a>
     * @param consoleCb If provided, messages are directed to it instead of
     *        stdout.
     */
    public DuccManagedReservationSubmit(ArrayList<String> args, IDuccCallback consoleCb)
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        serviceRequestProperties = new ServiceRequestProperties();   
        init (this.getClass().getName(), opts, arg_array, serviceRequestProperties, consoleCb);
    }
        
    /**
     * This form of the constructor allows the API user to capture
     * messages, rather than directing them to stdout. 
     *
     * @param props Properties file contianing string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_PROCESS_SUBMIT">DUCC CLI reference.</a>
     * @param consoleCb If provided, messages are directed to it instead of
     *        stdout.
     */
    public DuccManagedReservationSubmit(Properties props, IDuccCallback consoleCb) 
        throws Exception
    {
        serviceRequestProperties = new ServiceRequestProperties();
        init (this.getClass().getName(), opts, props, serviceRequestProperties, consoleCb);
    }

                        
    /**
     * Execute collects the job parameters, does basic error and correctness checking, and sends
     * the job properties to the DUCC orchestrator for execution.
     *
     * @return True if the orchestrator accepts the job; false otherwise.
     */
    public boolean execute() throws Exception 
    {
        // If the specified scheduling class is pre-emptable, change to a fixed one if possible
        String pname = UiOption.SchedulingClass.pname();
        String scheduling_class = serviceRequestProperties.getProperty(pname);
        if (scheduling_class != null) {
            DuccSchedulerClasses duccSchedulerClasses = DuccSchedulerClasses.getInstance();
            if (duccSchedulerClasses.isPreemptable(scheduling_class)) {
                scheduling_class = duccSchedulerClasses.getDebugClassSpecificName(scheduling_class);
                if (scheduling_class != null) {
                    serviceRequestProperties.setProperty(pname, scheduling_class);
                }
            }
        }
        
        // Create a copy to be saved later without these 3 "ducclet" properties required by DUCC
        ServiceRequestProperties serviceProperties = (ServiceRequestProperties)serviceRequestProperties.clone();
        serviceRequestProperties.setProperty(UiOption.ProcessThreadCount.pname(), "1");
        serviceRequestProperties.setProperty(UiOption.ProcessDeploymentsMax.pname(), "1");     
        serviceRequestProperties.setProperty(UiOption.ServiceTypeOther.pname(), "");
        
        SubmitServiceDuccEvent ev = new SubmitServiceDuccEvent(serviceRequestProperties, CliVersion.getVersion());
        SubmitServiceReplyDuccEvent reply = null;
        
        try {
            reply = (SubmitServiceReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }

        /*
         * process reply
         */
        boolean retval = true;
        Properties properties = reply.getProperties();
        @SuppressWarnings("unchecked")
        ArrayList<String> or_warnings = (ArrayList<String>) properties.get(UiOption.SubmitWarnings.pname());
        if (or_warnings != null) {
            for ( String s : or_warnings) {
               message("WARN:", s);
            }
        }

        @SuppressWarnings("unchecked")
        ArrayList<String> or_errors = (ArrayList<String>) properties.get(UiOption.SubmitErrors.pname());
        if(or_errors != null) {
            for ( String s : or_errors ) {
                message("ERROR:", s);
            }
            retval = false;
        }

        if ( retval ) {
            String pid = reply.getProperties().getProperty(UiOption.JobId.pname());
            if (pid == null ) {
                retval = false;
            } else {
                friendlyId = Long.parseLong(pid);
                if ( friendlyId < 0 ) {
                    retval = false;
                } else {
                    saveSpec(DuccUiConstants.managed_reservation_properties, serviceProperties);
                    startMonitors(true, DuccContext.ManagedReservation);       // starts conditionally, based on job spec and console listener present
                }
            }
        }

        return retval;
    }
        
    /**
     * Main method, as used by the executable jar or direct java invocation.
     * @param args arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_PROCESS_SUBMIT">DUCC CLI reference.</a>
     */
    public static void main(String[] args) 
    {
        int code = 1; // Assume the worst
        try {
            // Instantiate the object with args similar to the CLI, or a pre-built properties file
            DuccManagedReservationSubmit ds = new DuccManagedReservationSubmit(args);

            // Run the API.  If process_attach_console was specified in the args, a console listener is
            // started but this call does NOT block on it.
            boolean rc = ds.execute();

            // If the return is 'true' then as best the API can tell, the submit worked
            if ( rc ) {
                System.out.println(dt+" "+ds.getDuccId()+" submitted.");
                code = ds.getReturnCode();
            } else {
                System.out.println(dt+" Could not submit ");
            }
        } catch (Exception e) {
            System.out.println(dt+" Cannot initialize: " + e.getMessage());
        } finally {
            // Set the process exit code
            System.exit(code);
        }
    }
    
}
