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

import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;


/**
 * Submit a DUCC Managed Reservation
 */

public class DuccManagedReservationSubmit 
    extends CliBase
{
    private static String dt = "Managed Reservation";
    
    private static String or_port = "ducc.orchestrator.http.port";
    private static String or_host = "ducc.orchestrator.node";
   
    private ServiceRequestProperties serviceRequestProperties;

    private UiOption[] opts_release = new UiOption[] {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Description,
        UiOption.Environment,
        UiOption.LogDirectory,
        UiOption.ProcessAttachConsole,
        UiOption.ProcessExecutable,
        UiOption.ProcessExecutableArgs,
        UiOption.ProcessFailuresLimit,
        UiOption.ProcessMemorySize,
        UiOption.SchedulingClass,
        UiOption.Specification,
        UiOption.WorkingDirectory,
        UiOption.WaitForCompletion,
        UiOption.CancelOnInterrupt,
    };

    private UiOption[] opts_beta = new UiOption[] {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Description,
        UiOption.Environment,
        UiOption.LogDirectory,
        UiOption.ProcessAttachConsole,
        UiOption.ProcessEnvironment,
        UiOption.ProcessExecutable,
        UiOption.ProcessExecutableArgs,
        UiOption.ProcessFailuresLimit,
        UiOption.ProcessMemorySize,
        UiOption.SchedulingClass,
        UiOption.Specification,
        UiOption.WorkingDirectory,
        UiOption.WaitForCompletion,
        UiOption.CancelOnInterrupt,
        UiOption.CancelManagedReservationOnInterrupt,
    };
    
    private UiOption[] opts = opts_release;

    public DuccManagedReservationSubmit(String[] args)
        throws Exception
    {
        this(args, null);
    }
        
    public DuccManagedReservationSubmit(ArrayList<String> args)
        throws Exception
    {
        this(args, null);
    }
        
    public DuccManagedReservationSubmit(Properties props)
        throws Exception
    {
        this(props, null);
    }

    public DuccManagedReservationSubmit(String[] args, IDuccCallback consoleCb)
        throws Exception
    {
        serviceRequestProperties = new ServiceRequestProperties(); 
        init();
        if(DuccUiUtilities.isSupportedBeta()) {
        	opts = opts_beta;
        }
        init(this.getClass().getName(), opts, args, serviceRequestProperties, or_host, or_port, "or", consoleCb);
    }
        
    public DuccManagedReservationSubmit(ArrayList<String> args, IDuccCallback consoleCb)
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        serviceRequestProperties = new ServiceRequestProperties();   
        init();
        if(DuccUiUtilities.isSupportedBeta()) {
        	opts = opts_beta;
        }
        init(this.getClass().getName(), opts, arg_array, serviceRequestProperties, or_host, or_port, "or", consoleCb);
    }
        
    public DuccManagedReservationSubmit(Properties props, IDuccCallback consoleCb) 
        throws Exception
    {
        serviceRequestProperties = new ServiceRequestProperties();
        // TODO - can we get OR to just use properties and not these specialized classes?
        //        Until then, we need to pass in the right kind of props, sigh.
        for ( Object k : props.keySet() ) {      
            Object v = props.get(k);
            serviceRequestProperties.put(k, v);
        }
        init();
        if(DuccUiUtilities.isSupportedBeta()) {
        	opts = opts_beta;
        }
        init(this.getClass().getName(), opts, null, serviceRequestProperties, or_host, or_port, "or", consoleCb);
    }

                        
    public boolean execute() throws Exception 
    {

        // These must be enforced for "ducclets"
        serviceRequestProperties.setProperty(UiOption.ProcessThreadCount.pname(), "1");
        serviceRequestProperties.setProperty(UiOption.ProcessDeploymentsMax.pname(), "1");     
        serviceRequestProperties.put(UiOption.ServiceTypeOther.pname(), "");
        
        /*
         * set DUCC_LD_LIBRARY_PATH in process environment
         */
        boolean ev0 = serviceRequestProperties.containsKey(UiOption.Environment.pname());
        boolean evp = serviceRequestProperties.containsKey(UiOption.ProcessEnvironment.pname());
        if(ev0 && evp) {
        	throw new IllegalArgumentException("Conflict: cannot specify both "+UiOption.Environment.pname()+" and "+UiOption.ProcessEnvironment.pname());
        }
        if(ev0) {
        	if (!DuccUiUtilities.ducc_environment(this, serviceRequestProperties, UiOption.Environment.pname())) {
    			return false;
    		}
        }
        else {
        	if(evp) {
        		if (!DuccUiUtilities.ducc_environment(this,serviceRequestProperties, UiOption.ProcessEnvironment.pname())) {
        			return false;
        		}
        	}
        	else {
        		if (!DuccUiUtilities.ducc_environment(this, serviceRequestProperties, UiOption.Environment.pname())) {
        			return false;
        		}
        	}
        }

        SubmitServiceDuccEvent ev = new SubmitServiceDuccEvent(serviceRequestProperties);
        SubmitServiceReplyDuccEvent reply = null;
        
        try {
            reply = (SubmitServiceReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            message("ERROR:", dt, "not submitted:", e.toString());
            return false;
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
                    saveSpec(DuccUiConstants.managed_reservation_properties, serviceRequestProperties);
                    startMonitors(true, DuccContext.ManagedReservation);       // starts conditionally, based on job spec and console listener present
                }
            }
        }

        return retval;
    }
        
    public static void main(String[] args) 
    {
        try {
            // Instantiate the object with args similar to the CLI, or a pre-built properties file
            DuccManagedReservationSubmit ds = new DuccManagedReservationSubmit(args);

            // Run the API.  If process_attach_console was specified in the args, a console listener is
            // started but this call does NOT block on it.
            boolean rc = ds.execute();

            // If the return is 'true' then as best the API can tell, the submit worked
            if ( rc ) {
                // Fetch the Ducc ID
            	System.out.println(dt+" "+ds.getDuccId()+" submitted.");
            	int code = 0;
            	if(ds.waitForCompletion()) {
            		code = ds.getReturnCode();
            	}
                if ( code == 1000 ) {
                    System.out.println(dt + ": no return code.");
                } else {
                    System.out.println(dt+" return code: "+code);
                }
            	System.exit(0);
            } else {
                System.out.println("Could not submit "+dt);
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("Cannot initialize: " + e.getMessage());
            System.exit(1);
        }
    }
    
}
