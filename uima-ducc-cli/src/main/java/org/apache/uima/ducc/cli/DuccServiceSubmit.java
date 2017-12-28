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
import org.apache.uima.ducc.transport.event.sm.IService.ServiceType;


/**
 * Submit a DUCC service.  This is usually called by the DUCC Service Manager but is
 * made public to enable developer-driven management and testing of services without
 * formal service registration.
 */

public class DuccServiceSubmit
    extends CliBase
{

    //private String jvmarg_string = null;
    //private Properties jvmargs = null;
    ServiceRequestProperties requestProperties = new ServiceRequestProperties();

    UiOption[] opts = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Description,
        UiOption.Administrators,      // ( not used directly here, but is allowed in registration )

        UiOption.SchedulingClass,
        UiOption.WorkingDirectory,   // Must precede LogDirecory
        UiOption.LogDirectory,       // Must precede Environment
        UiOption.Jvm,
        UiOption.ProcessJvmArgs,
        UiOption.Classpath,
        UiOption.Environment,
        UiOption.ProcessMemorySize,
        UiOption.ProcessDD,
        UiOption.ProcessExecutable,
        UiOption.ProcessExecutableArgs,
        UiOption.ProcessInitializationTimeMax,

        UiOption.ProcessDebug,

        UiOption.InstanceFailureLimit,
        UiOption.Specification,
        UiOption.ServiceDependency,
        UiOption.ServiceRequestEndpoint,
        UiOption.ServiceLinger,

        UiOption.ServicePingArguments,
        UiOption.ServiceId,
        UiOption.ServicePingClass,
        UiOption.ServicePingClasspath,
        UiOption.ServicePingJvmArgs,
        UiOption.ServicePingTimeout,
        UiOption.ServicePingDoLog,

        UiOption.InstanceFailureWindow,
        UiOption.InstanceFailureLimit,
        UiOption.InstanceInitFailureLimit,
    };

    /**
     * @param args Array of string arguments as described in the
     *             Command Line Interface section of the DuccBook
     * @throws Exception if the request is invalid
     */
    public DuccServiceSubmit(String[] args)
        throws Exception
    {
        init(this.getClass().getName(), opts, args, requestProperties, null);
    }

    /**
     * @param args List of string arguments as described in the
     *             Command Line Interface section of the DuccBook
     * @throws Exception if the request is invalid
     */
    public DuccServiceSubmit(ArrayList<String> args)
        throws Exception
    {
        this(args.toArray(new String[args.size()]));
    }

    /**
     * @param props Properties file of arguments, as described in the
     *              Command Line Interface section of the DuccBook
     * @throws Exception if the request is invalid
     */
    public DuccServiceSubmit(Properties props)
        throws Exception
    {
        init (this.getClass().getName(), opts, props, requestProperties, null);
    }

    protected void enrich_parameters_for_debug(Properties props)
        throws Exception
    {
        try {
            int debug_port = -1;
            String debug_host = null;

            // we allow both jd and jp to debug, but the ports have to differ
            String do_debug = UiOption.ProcessDebug.pname();
            if ( props.containsKey(do_debug) ) {
                String deb = props.getProperty(do_debug);
                if ( deb == null ) {
                    throw new IllegalArgumentException("Missing port for " + do_debug);
                }

                if ( deb.equals("off") ) {
                    System.out.println("Note: Ignoring process_debug = off");
                    return;
                }

                String[] parts = deb.split(":");
                if ( parts.length != 2 ) {
                    System.out.println("Warning: process_debug must be of the form host: port.  Found '" + deb + "'.  Ignoring debug.");
                    return;
                }

                debug_host = parts[0];
                debug_port = Integer.parseInt(parts[1]);

                String debug_jvmargs = "-Xdebug -Xrunjdwp:transport=dt_socket,address=" + debug_host + ":" + debug_port;
                String jvmargs = props.getProperty(UiOption.ProcessJvmArgs.pname());
                if (jvmargs == null) {
                    jvmargs = debug_jvmargs;
                } else {
                    jvmargs += " " + debug_jvmargs;
                }
                props.put(UiOption.ProcessJvmArgs.pname(), jvmargs);

                // For debugging, if the JP is being debugged, insure these are conservative
                props.setProperty(UiOption.ProcessDeploymentsMax.pname(), "1");
                props.setProperty(UiOption.ProcessFailuresLimit.pname(), "1");
            }

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid debug port (not numeric)");
        }
    }

    /**
     * Execute collects the service parameters, does basic error and correctness checking, and sends
     * the job properties to the DUCC orchestrator for execution.
     *
     * @return True if the orchestrator accepts the service; false otherwise.
     */
    public boolean execute()
        throws Exception
    {
        //
        // Need to check if the mutually exclusive UIMA-AS DD and the Custom executable are specified
        //
        String uimaDD = cli_props.getStringProperty(UiOption.ProcessDD.pname(), null);
        String customCmd = cli_props.getStringProperty(UiOption.ProcessExecutable.pname(), null);

        String endpoint = requestProperties.getProperty(UiOption.ServiceRequestEndpoint.pname());
        
        // No need to validate endpoint here as was done when registered
        if (endpoint == null) {
            message("ERROR: the endpoint should have been set and verified during registration");
            return false;
        }
        
        boolean isUimaAs = true;

        if (endpoint.startsWith(ServiceType.UimaAs.decode())) {
            requestProperties.put(UiOption.ServiceTypeUima.pname(), "");
            if (uimaDD == null) {
                message("ERROR: Must specify --process_DD for UIMA-AS services");
                return false;
            }
            if (customCmd != null) {
                message("WARN: --process_executable is ignored for UIMA-AS services");
            }

            // The classpath should have been set during registration ... the SM's classpath won't help!
            String key_cp = UiOption.Classpath.pname();
            if (!cli_props.containsKey(key_cp)) {
                message("ERROR: the classpath should have been set during registration");
                return false;
            }

            // No need to open DD and validate endpoint here as was done when registered

        } else if (endpoint.startsWith(ServiceType.Custom.decode())) {
            isUimaAs = false;
            if (uimaDD != null) {
                message("WARN: --process_DD is ignored for CUSTOM endpoints");
            }
            requestProperties.put(UiOption.ServiceTypeCustom.pname(), "");

        } else {
            return false;
        }

        if ( ! check_service_dependencies(endpoint) ) {
            return false;
        }

        if ( debug ) {
            requestProperties.dump();
        }

        if ( isUimaAs ) {
            enrich_parameters_for_debug(requestProperties);
        }

        requestProperties.put(UiOption.ProcessPipelineCount.pname(), "1");         // enforce this - OR will complain if it's missing

        // Note: context is provided for system event logger to disambiguate Service from ManagedReservation
        SubmitServiceDuccEvent      ev    = new SubmitServiceDuccEvent(requestProperties, DuccContext.Service, CliVersion.getVersion());
        SubmitServiceReplyDuccEvent reply = null;

        try {
            reply = (SubmitServiceReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            message("Service instance not submitted:", e.getMessage());
            return false;
        } finally {
            dispatcher.close();
        }

        /*
         * process reply
         */
        boolean rc = extractReply(reply);

        if ( rc ) {
            saveSpec(DuccUiConstants.service_specification_properties, requestProperties);
        }

        return rc;
    }

    /**
     * Main method, as used by the executable jar or direct java invocation.
     * @param args arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SERVICE_SUBMIT">DUCC CLI reference.</a>
     */
    public static void main(String[] args) {
        try {
            // Instantiate the object with args similar to the CLI, or a pre-built properties file
            DuccServiceSubmit ds = new DuccServiceSubmit(args);

            // Run the API.  If process_attach_console was specified in the args, a console listener is
            // started but this call does NOT block on it.
            boolean rc = ds.execute();

            // If the return is 'true' then as best the API can tell, the submit worked
            if ( rc ) {

                // Fetch the Ducc ID
                System.out.println("Service instance " + ds.getDuccId() + " submitted");
                System.exit(0);
            } else {
                System.out.println("Could not submit Service");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("Cannot initialize: " + e.getMessage());
            System.exit(1);
        }

    }

}
