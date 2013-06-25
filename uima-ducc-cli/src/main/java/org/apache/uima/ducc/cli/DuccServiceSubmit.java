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

import org.apache.uima.ducc.cli.IServiceApi.RegistrationOption;
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
    static String or_port = "ducc.orchestrator.http.port";
    static String or_host = "ducc.orchestrator.node";
    
    UiOption[] opts_release = {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Description,
        UiOption.SchedulingClass,
        UiOption.LogDirectory,
        UiOption.WorkingDirectory,
        UiOption.Jvm,
        UiOption.JvmArgs,
        UiOption.Classpath,
        UiOption.Environment,
        UiOption.ProcessMemorySize,
        UiOption.ProcessDD,
        UiOption.ProcessExecutable,
        UiOption.ProcessExecutableArgs,
        UiOption.ProcessFailuresLimit,
        UiOption.ClasspathOrder,
        UiOption.Specification,
        UiOption.ServiceDependency,
        UiOption.ServiceRequestEndpoint,
        UiOption.ServiceLinger,
        UiOption.ServicePingArguments,
        UiOption.ServicePingClass,
        UiOption.ServicePingClasspath,
        UiOption.ServicePingJvmArgs,
        UiOption.ServicePingTimeout,
        UiOption.ServicePingDoLog,
    };
    
    UiOption[] opts_beta = {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Description,
        UiOption.SchedulingClass,
        UiOption.LogDirectory,
        UiOption.WorkingDirectory,
        UiOption.Jvm,
        UiOption.JvmArgs,
        UiOption.Classpath,
        UiOption.Environment,
        UiOption.ProcessJvmArgs,
        UiOption.ProcessClasspath,
        UiOption.ProcessEnvironment,
        UiOption.ProcessMemorySize,
        UiOption.ProcessDD,
        UiOption.ProcessExecutable,
        UiOption.ProcessExecutableArgs,
        UiOption.ProcessFailuresLimit,
        UiOption.ClasspathOrder,
        UiOption.Specification,
        UiOption.ServiceDependency,
        UiOption.ServiceRequestEndpoint,
        UiOption.ServiceLinger,
        UiOption.ServicePingArguments,
        UiOption.ServicePingClass,
        UiOption.ServicePingClasspath,
        UiOption.ServicePingJvmArgs,
        UiOption.ServicePingTimeout,
        UiOption.ServicePingDoLog,
    };
    
    UiOption[] opts = opts_release;

    /**
     * @param args Array of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_SERVICE_SUBMIT">DUCC CLI reference.</a>
     */
    public DuccServiceSubmit(String[] args)
        throws Exception
    {
        init();
        if(DuccUiUtilities.isSupportedBeta()) {
            opts = opts_beta;
        }
        init(this.getClass().getName(), opts, args, requestProperties, or_host, or_port, "or", null, null);
    }
    
    /**
     * @param args List of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_SERVICE_SUBMIT">DUCC CLI reference.</a>
     */
    public DuccServiceSubmit(ArrayList<String> args)
        throws Exception
    {
        this(args.toArray(new String[args.size()]));
    }

    /**
     * @param props Properties file of arguments, as described in the
     *      <a href="/doc/duccbook.html#DUCC_CLI_SERVICE_SUBMIT">DUCC CLI reference.</a>
     */
    public DuccServiceSubmit(Properties props)
        throws Exception
    {
        for ( Object k : props.keySet() ) {      
            Object v = props.get(k);
            requestProperties.put(k, v);
        }
        init();
        if(DuccUiUtilities.isSupportedBeta()) {
            opts = opts_beta;
        }
        init(this.getClass().getName(), opts, null, requestProperties, or_host, or_port, "or", null, null);
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
        String key_ja = UiOption.ProcessJvmArgs.pname();
        if ( cli_props.containsKey(UiOption.JvmArgs.pname()) ) {
            key_ja = UiOption.JvmArgs.pname();
        }
        String jvmarg_string = (String) requestProperties.get(key_ja);
        Properties jvmargs = DuccUiUtilities.jvmArgsToProperties(jvmarg_string);

        //
        // Need to check if the mutually exclusive UIMA-AS DD and the Custom executable are specified
        //
        String uimaDD = cli_props.getStringProperty(UiOption.ProcessDD.pname(), null);
        String customCmd = cli_props.getStringProperty(UiOption.ProcessExecutable.pname(), null);
        
        String endpoint = requestProperties.getProperty(RegistrationOption.ServiceRequestEndpoint.decode());
        
        if (endpoint == null || endpoint.startsWith(ServiceType.UimaAs.decode())) {
            requestProperties.put(UiOption.ServiceTypeUima.pname(), "");
            if (uimaDD == null) {
                message("ERROR: Must specify --process_DD for UIMA-AS services");
                return false;
            }
            if (customCmd != null) {
                message("WARN: --process_executable is ignored for UIMA-AS services");
            }
            //
            // Always extract the endpoint from the DD since when it is explicitly specified it must match.
            //
            try {
                String dd = (String) requestProperties.get(UiOption.ProcessDD.pname());
                String wd = (String) requestProperties.get(UiOption.WorkingDirectory.pname());
                String inferred_endpoint = DuccUiUtilities.getEndpoint(wd, dd, jvmargs);
                if (endpoint == null) {
                    endpoint = inferred_endpoint;
                    requestProperties.put(UiOption.ServiceRequestEndpoint.pname(), endpoint);
                } else if (!inferred_endpoint.equals(endpoint)) {
                    message("ERROR: Endpoint from --service_request_endpoint does not match endpoint ectracted from UIMA DD"
                                    + "\n--service_request_endpoint: "
                                    + endpoint
                                    + "\nextracted:                : " + inferred_endpoint);
                    return false;
                }
                if (debug) {
                    System.out.println("service_endpoint: " + endpoint);
                }
            } catch (IllegalArgumentException e) {
                message("ERROR: Cannot read/process DD descriptor for endpoint:", e.getMessage());
                return false;
            }

        } else if (endpoint.startsWith(ServiceType.Custom.decode())) {
            if (uimaDD != null) {
                message("WARN: --process_DD is ignored for CUSTOM endpoints");
            }
            requestProperties.put(UiOption.ServiceTypeCustom.pname(), "");

        } else {
            return false;
        }

        if ( ! resolve_service_dependencies(endpoint) ) {            
            return false;
        }
        
        if ( debug ) {
            requestProperties.dump();
        }
    
        /*
         * set DUCC_LD_LIBRARY_PATH in process environment
         */
        String key_ev = UiOption.ProcessEnvironment.pname();
        if ( cli_props.containsKey(UiOption.Environment.pname()) ) {
            key_ev = UiOption.Environment.pname();
        }
        if (!DuccUiUtilities.ducc_environment(this, requestProperties, key_ev)) {
            return false;
        }
        requestProperties.put(UiOption.ProcessThreadCount.pname(), "1");         // enforce this - OR will complain if it's missing

        SubmitServiceDuccEvent      ev    = new SubmitServiceDuccEvent(requestProperties);
        SubmitServiceReplyDuccEvent reply = null;
        
        try {
            reply = (SubmitServiceReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            message("Process not submitted:", e.getMessage());
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
                System.out.println("Service " + ds.getDuccId() + " submitted");
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
