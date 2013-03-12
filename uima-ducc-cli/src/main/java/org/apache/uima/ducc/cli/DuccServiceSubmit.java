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
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceType;


/**
 * Submit a DUCC service
 */

public class DuccServiceSubmit 
    extends CliBase
{
	
    //private String jvmarg_string = null;
    //private Properties jvmargs = null;
    ServiceRequestProperties requestProperties = new ServiceRequestProperties();
    static String or_port = "ducc.orchestrator.http.port";
    static String or_host = "ducc.orchestrator.node";
	
    UiOption[] opts = new UiOption[] {
        UiOption.Help,
        UiOption.Debug, 

        UiOption.Description,
        UiOption.SchedulingClass,
        UiOption.LogDirectory,
        UiOption.WorkingDirectory,
        UiOption.Jvm,
        UiOption.ProcessJvmArgs,
        UiOption.ProcessClasspath,
        UiOption.ProcessEnvironment,
        UiOption.ProcessMemorySize,
        UiOption.ProcessDD,
        UiOption.ProcessInitializationFailuresCap,
        UiOption.ProcessFailuresLimit,
        UiOption.ClasspathOrder,
        UiOption.Specification,
        UiOption.ServiceDependency,
        UiOption.ServiceRequestEndpoint,
        UiOption.ServiceLinger,
        UiOption.ServicePingClass,
        UiOption.ServicePingClasspath,
        UiOption.ServicePingJvmArgs,
        UiOption.ServicePingTimeout,
        UiOption.ServicePingDoLog,
        UiOption.ProcessGetMetaTimeMax,
    };
 
	public DuccServiceSubmit(ArrayList<String> args)
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        init(this.getClass().getName(), opts, arg_array, requestProperties, or_host, or_port, "or", null, "services");
    }

	public DuccServiceSubmit(String[] args)
        throws Exception
    {
        init(this.getClass().getName(), opts, args, requestProperties, or_host, or_port, "or", null, "services");
    }

	public DuccServiceSubmit(Properties props)
        throws Exception
    {
        for ( Object k : props.keySet() ) {      
            Object v = props.get(k);
            requestProperties.put(k, v);
        }
        init(this.getClass().getName(), opts, null, requestProperties, or_host, or_port, "or", null, "services");
    }
	
    // TODO: if uima-as, then DD is required
		
    private ServiceDeploymentType getServiceType(ServiceRequestProperties requestProperties)
    {
        // if the service type is NOT set, then it has to be some kind of service, see if there's an  endpoint
        String service_endpoint = requestProperties.getProperty(RegistrationOption.ServiceRequestEndpoint.decode());

        // No end point, it HAS to be UIMA-AS and therefore requires a DD to be valid
        if (service_endpoint == null) {            
            String dd = (String) requestProperties.get(UiOption.ProcessDD.pname());
            if ( dd != null ) {
                return ServiceDeploymentType.uima;
            } else {
                throw new IllegalArgumentException("Missing service endpoint and DD, cannot identify service type.");
            }
        }

        if ( service_endpoint.startsWith(ServiceType.Custom.decode()) ) {
            return ServiceDeploymentType.custom;
        }

        if ( service_endpoint.startsWith(ServiceType.UimaAs.decode()) ) {
            return ServiceDeploymentType.uima;
        }


        throw new IllegalArgumentException("Invalid service type in endpoint, must be " + ServiceType.UimaAs.decode() + " or " + ServiceType.Custom.decode() + ".");
    }


	
	@SuppressWarnings("unused")
	public boolean execute() 
        throws Exception 
    {
        String jvmarg_string = (String) requestProperties.get(UiOption.ProcessJvmArgs.pname());
        Properties jvmargs = DuccUiUtilities.jvmArgsToProperties(jvmarg_string);

        ServiceDeploymentType deploymentType = null;
        try {
            deploymentType = getServiceType(requestProperties);
        } catch ( Throwable t ) {
            addError(t.getMessage());
            return false;
        }

        String service_endpoint = null;
		switch ( deploymentType ) 
        {
            case uima:
                requestProperties.put(UiOption.ServiceTypeUima.pname(), "");
                if(service_endpoint == null) {
                    // A null endpoint means it MUST be UimaAs and we are going to derive it.  Otherwise it's the user's responsibility to
                    // have it set correctly, because really can't tell.                    
                    //
                    // The service endpoint is extracted from the DD. It is for internal use only, not publicly settable or documented.
                    //
                    try {
                        String dd = (String) requestProperties.get(UiOption.ProcessDD.pname());
                        String wd = (String) requestProperties.get(UiOption.WorkingDirectory.pname());
                        //System.err.println("DD: " + dd);
                        //System.err.println("WD: " + wd);
                        //System.err.println("jvmargs: " + jvmarg_string);
                        service_endpoint = DuccUiUtilities.getEndpoint(wd, dd, jvmargs);
                        requestProperties.put(UiOption.ServiceRequestEndpoint.pname(), service_endpoint);
                        if( debug ) {
                            System.out.println("service_endpoint:"+" "+service_endpoint);
                        }
                    } catch ( IllegalArgumentException e ) {
                        addError("Cannot read/process DD descriptor for endpoint: " + e.getMessage());
                        return false;
                    }
                } else {
                    requestProperties.put(UiOption.ServiceRequestEndpoint.pname(), service_endpoint);
                }
                break;

            case custom:
                requestProperties.put(UiOption.ServiceTypeCustom.pname(), "");
                if ( service_endpoint == null ) {
                    addError("Missing endpoint for CUSTOM service.");
                    return false;
                } else {
                    requestProperties.put(UiOption.ServiceRequestEndpoint.pname(), service_endpoint);
                }
                break;
            
            case unspecified:
                return false;
        } 

        if ( ! resolve_service_dependencies(service_endpoint) ) {            
            return false;
        }
        
		if ( debug ) {
			requestProperties.dump();
		}
	
        /*
		 * set DUCC_LD_LIBRARY_PATH in process environment
		 */
		if (!DuccUiUtilities.ducc_environment(this, requestProperties, UiOption.ProcessEnvironment.pname())) {
            return false;
		}
        requestProperties.put(UiOption.ProcessThreadCount.pname(), "1");         // enforce this - OR will complain if it's missing

        SubmitServiceDuccEvent      ev    = new SubmitServiceDuccEvent(requestProperties);
        SubmitServiceReplyDuccEvent reply = null;
        
        try {
            reply = (SubmitServiceReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            addError("Process not submitted: " + e.getMessage());
            return false;
        } finally {
            dispatcher.close();
        }

        /*
         * process reply
         */
        boolean rc = extractReply(reply);

        if ( rc ) {
            saveSpec("service-specification.properties", requestProperties);
        }

		return rc;
    }
		
	public static void main(String[] args) {
        try {
            // Instantiate the object with args similar to the CLI, or a pre-built properties file
            DuccServiceSubmit ds = new DuccServiceSubmit(args);            

            // Run the API.  If process_attach_console was specified in the args, a console listener is
            // started but this call does NOT block on it.
            boolean rc = ds.execute();

            // Fetch messages if any.  null means none
            String [] messages = ds.getMessages();
            String [] warnings = ds.getWarnings();
            String [] errors   = ds.getErrors();

            if ( messages != null ) {
                for (String s : messages ) {
                    System.out.println(s);
                }
            }

            if ( warnings != null ) {
                for (String s : warnings ) {
                    System.out.println("WARN: " + s);
                }
            }

            if ( errors != null ) {
                for (String s : errors ) {
                    System.out.println("ERROR: " + s);
                }
            }

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
