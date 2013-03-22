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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;

/**
 * Submit a DUCC job
 */

public class DuccJobSubmit 
    extends CliBase 
{	
    private JobRequestProperties jobRequestProperties = new JobRequestProperties();        
    static String or_port = "ducc.orchestrator.http.port";
    static String or_host = "ducc.orchestrator.node";
	
	// public DuccJobSubmit(IDuccMessageProcessor duccMessageProcessor) {
// 		this.duccMessageProcessor = duccMessageProcessor;
// 	}

    UiOption[] opts = new UiOption[] {
        UiOption.Help,
        UiOption.Debug, 

        UiOption.ProcessDebug,
        UiOption.ProcessAttachConsole,
        UiOption.DriverDebug,
        UiOption.DriverAttachConsole,
        UiOption.Timestamp,
        UiOption.Description,
        UiOption.SchedulingClass,

        UiOption.LogDirectory,
        UiOption.WorkingDirectory,
        UiOption.Jvm,

        UiOption.DriverJvmArgs,
        UiOption.DriverClasspath,
        UiOption.DriverEnvironment,
        UiOption.DriverDescriptorCR,
        UiOption.DriverDescriptorCROverrides,
        UiOption.DriverExceptionHandler,

        UiOption.ProcessJvmArgs,
        UiOption.ProcessClasspath,
        UiOption.ProcessMemorySize,

        UiOption.ProcessEnvironment,
        UiOption.ProcessDD,
        UiOption.ProcessDescriptorCM,
        UiOption.ProcessDescriptorCMOverrides,
        UiOption.ProcessDescriptorAE,
        UiOption.ProcessDescriptorAEOverrides,
        UiOption.ProcessDescriptorCC,
        UiOption.ProcessDescriptorCCOverrides,
        
        UiOption.ProcessDeploymentsMax,
        UiOption.ProcessInitializationFailuresCap,
        UiOption.ProcessFailuresLimit,
        UiOption.ProcessThreadCount,
        UiOption.ProcessPerItemTimeMax,
        UiOption.ProcessGetMetaTimeMax,

        UiOption.Specification,
        UiOption.WaitForCompletion,
        UiOption.CancelJobOnInterrupt,
        UiOption.ServiceDependency,
        UiOption.ClasspathOrder,
    };


	public DuccJobSubmit(ArrayList<String> args)
        throws Exception
    {
        this(args, null);
    }

	public DuccJobSubmit(String[] args)
        throws Exception
    {
        this(args, null);
    }

	public DuccJobSubmit(Properties props)
        throws Exception
    {
        this(props, null);
    }

	public DuccJobSubmit(ArrayList<String> args, IConsoleCallback consoleCb)
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        init(this.getClass().getName(), opts, arg_array, jobRequestProperties, or_host, or_port, "or", consoleCb, null);
    }

	public DuccJobSubmit(String[] args, IConsoleCallback consoleCb)
        throws Exception
    {
        init(this.getClass().getName(), opts, args, jobRequestProperties, or_host, or_port, "or", consoleCb, null);
    }

	public DuccJobSubmit(Properties props, IConsoleCallback consoleCb)
        throws Exception
    {
        for ( Object k : props.keySet() ) {      
            Object v = props.get(k);
            jobRequestProperties.put(k, v);
        }
        init(this.getClass().getName(), opts, null, jobRequestProperties, or_host, or_port, "or", consoleCb, null);
    }

    private void set_debug_parms(Properties props, String key, int port)
    {
        String debug_address = host_address + ":" + port;
        String jvmargs = props.getProperty(key);
        jvmargs = jvmargs + " -Xdebug";
        jvmargs = jvmargs + " -Xrunjdwp:transport=dt_socket,address=" + debug_address;            
        props.put(key, jvmargs);
    }
    
    protected void enrich_parameters_for_debug(Properties props)
        throws Exception
    {
        
        try {        
            int jp_debug_port = -1;
            int jd_debug_port = -2;       // a trick, must be different from jp_debug_port; see below

            // we allow both jd and jp to debug, but the ports have to differ
            String do_debug = UiOption.ProcessDebug.pname();
            if ( props.containsKey(do_debug) ) {
                String jp_port_s = props.getProperty(do_debug);
                if ( jp_port_s == null ) {
                    throw new IllegalArgumentException("Missing port for " + do_debug);
                }
                jp_debug_port = Integer.parseInt(jp_port_s);
                
                set_debug_parms(props, UiOption.ProcessJvmArgs.pname(), jp_debug_port);
                // For debugging, if the JP is being debugged, we have to force max processes to .1
                props.setProperty(UiOption.ProcessDeploymentsMax.pname(), "1");
            }

            do_debug = UiOption.DriverDebug.pname();
            if ( props.containsKey(do_debug) ) {
                String jd_port_s = props.getProperty(do_debug);
                if ( jd_port_s == null ) {
                    throw new IllegalArgumentException("Missing port for " + do_debug);
                }
                jd_debug_port = Integer.parseInt(jd_port_s);
                set_debug_parms(props, UiOption.ProcessJvmArgs.pname(), jd_debug_port);
            }
            
            if ( jp_debug_port == jd_debug_port ) {
                throw new IllegalArgumentException("Process and Driver debug ports must differ.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid debug port (not numeric)");
        }

    }
	
	private long getThreadsLimit() {
		long limit = 0;
		try {
			String p_limit = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_submit_threads_limit);
			if(p_limit != null) {
				p_limit = p_limit.trim();
				if(!p_limit.equals("unlimited")) {
					limit = Long.parseLong(p_limit);
				}
			}
		}
		catch(Throwable t) {
            addThrowable(t);
		}
		return limit;
	}
	
	private boolean adjust_max_threads(Properties properties) {
		boolean retVal = false;
		try {
			long limit = getThreadsLimit();
			if(limit == 0) {
				return retVal;
			}
			String p_threads = properties.getProperty(UiOption.ProcessThreadCount.pname());
			if(p_threads == null) {
				p_threads = UiOption.ProcessThreadCount.deflt();;
			}

			long threads = Long.parseLong(p_threads);
			String p_procs = properties.getProperty(UiOption.ProcessDeploymentsMax.pname());
			if(p_procs == null) {
				long procs = limit / threads;
				p_procs = "unlimited";
				String a_procs = ""+procs;
				addMessage(UiOption.ProcessDeploymentsMax.pname() + ": requested[" + p_procs + "] adjusted to[" + a_procs + "]");
				properties.setProperty(UiOption.ProcessDeploymentsMax.pname(), a_procs);
				retVal = true;
			}
			else {
				long procs = Long.parseLong(p_procs);
				if( (procs * threads) > limit ) {
					procs = limit / threads;
					String a_procs = ""+procs;
					addMessage(UiOption.ProcessDeploymentsMax.pname()+": requested["+p_procs+"] adjusted to["+a_procs + "]");
					properties.setProperty(UiOption.ProcessDeploymentsMax.pname(), a_procs);
					retVal = true;
				}
			}
		}
		catch(Throwable t) {
            addThrowable(t);
		}
		return retVal;
	}
	
	//**********
	
	private String getDuccProperty(String propertyName, String defaultValue) {
		String propertyValue = defaultValue;
		try {
			String value = DuccPropertiesResolver.getInstance().getProperty(propertyName);
			if(value != null) {
				propertyValue = value;
			}
		}
		catch(Throwable t) {
            addThrowable(t);
		}
		return propertyValue;
	}
	
    private void adjust_specific_jvm_args(String additionalArgs, UiOption args_option)
    {
		String additionalJvmArgs = getDuccProperty(additionalArgs, null);
		if(additionalJvmArgs != null) {
			String jvmArgs = jobRequestProperties.getProperty(args_option.pname());
			if(jvmArgs == null) {
				jvmArgs = additionalJvmArgs;
			}
			else {
				jvmArgs += " "+additionalJvmArgs;
			}
			jobRequestProperties.setProperty(args_option.pname(), jvmArgs);
		}

    }
	
	private void adjust_jvm_args(Properties jobRequestProperties) {
		adjust_specific_jvm_args(DuccPropertiesResolver.ducc_submit_driver_jvm_args, UiOption.DriverJvmArgs);
		adjust_specific_jvm_args(DuccPropertiesResolver.ducc_submit_process_jvm_args, UiOption.ProcessJvmArgs);
	}

	//**********		
	
	public boolean execute() 
        throws Exception 
    {
                    
        try {
			enrich_parameters_for_debug(jobRequestProperties);
        } catch (Exception e1) {
            addThrowable(e1);
            return false;
		}
        
		/*
		 * employ default driver classpath if not specified
		 */
		String driver_classpath = jobRequestProperties.getProperty(UiOption.DriverClasspath.pname());
		if(driver_classpath == null) {
			driver_classpath = System.getProperty("java.class.path");
			jobRequestProperties.setProperty(UiOption.DriverClasspath.pname(), driver_classpath);
		}

		/*
		 * employ default process classpath if not specified
		 */
		String process_classpath = jobRequestProperties.getProperty(UiOption.ProcessClasspath.pname());
		if(process_classpath == null) {
			process_classpath = System.getProperty("java.class.path");
			jobRequestProperties.setProperty(UiOption.ProcessClasspath.pname(), process_classpath);
		}
        
		/*
		 * employ default process initialization failures cap if not specified
		 */
		String process_initialization_failures_cap = jobRequestProperties.getProperty(UiOption.ProcessInitializationFailuresCap.pname());
		if(process_initialization_failures_cap == null) {
			jobRequestProperties.setProperty(UiOption.ProcessInitializationFailuresCap.pname(), UiOption.ProcessInitializationFailuresCap.deflt());
		}

		/*
		 * employ default process failures limit if not specified
		 */
		String process_failures_limit = jobRequestProperties.getProperty(UiOption.ProcessFailuresLimit.pname());
		if(process_failures_limit == null) {
			jobRequestProperties.setProperty(UiOption.ProcessFailuresLimit.pname(), UiOption.ProcessFailuresLimit.deflt());
		}
		if (jobRequestProperties.containsKey(UiOption.Debug.pname())) {
			jobRequestProperties.dump();
		}

		/*
		 * set DUCC_LD_LIBRARY_PATH in driver, process environment
		 */
		if (!DuccUiUtilities.ducc_environment(this, jobRequestProperties, UiOption.DriverEnvironment.pname())) {
			return false;
		}
		
		if (!DuccUiUtilities.ducc_environment(this, jobRequestProperties, UiOption.ProcessEnvironment.pname())) {
			return false;
		}

		/*
		 * limit total number of threads
		 */
		adjust_max_threads(jobRequestProperties);

		/*
		 * adjust driver and process jvm args
		 */
		adjust_jvm_args(jobRequestProperties);

		/*
		 * identify invoker
		 */
		jobRequestProperties.setProperty(UiOption.SubmitPid.pname(), ManagementFactory.getRuntimeMXBean().getName());

        /*
         * resolve ${defaultBrokerURL} in service dependencies - must fail if resolution needed but can't resolve
         */
        if ( ! resolve_service_dependencies(null) ) {
            return false;
        }

        SubmitJobDuccEvent      submitJobDuccEvent      = new SubmitJobDuccEvent(jobRequestProperties);
        SubmitJobReplyDuccEvent submitJobReplyDuccEvent = null;
        try {
            submitJobReplyDuccEvent = (SubmitJobReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(submitJobDuccEvent);
        } catch (Exception e) {
            addError("Job not submitted: " + e.getMessage());
            stopListeners();
            return false;
        } finally {
            dispatcher.close();
        }

        /*
         * process reply
         */
        boolean rc = extractReply(submitJobReplyDuccEvent);

        if ( rc ) {
            saveSpec(DuccUiConstants.job_specification_properties, jobRequestProperties);
            startMonitors(false, DuccContext.Job);       // starts conditionally, based on job spec and console listener present
        }

		return rc;
	}
	
	public static void main(String[] args) {
		try {
			DuccJobSubmit ds = new DuccJobSubmit(args, null);
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
            	System.out.println("Job " + ds.getDuccId() + " submitted");
                int exit_code = 0;          // first best guess, waiting for completion.
                if ( ds.waitForCompletion() ) {
                    exit_code = ds.getReturnCode();       // updated from wait.
                    System.out.println("Job return code: " + exit_code);
                }
            	System.exit(exit_code);
            } else {
                System.out.println("Could not submit job");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("Cannot initialize: " + e.getMessage());
            System.exit(1);
        }
	}
}
