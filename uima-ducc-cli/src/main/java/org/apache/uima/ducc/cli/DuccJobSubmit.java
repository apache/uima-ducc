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

import org.apache.commons.cli.MissingArgumentException;
import org.apache.uima.ducc.cli.aio.AllInOneLauncher;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
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
//         this.duccMessageProcessor = duccMessageProcessor;
//     }
    
    static UiOption[] opts_release = new UiOption[] {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Timestamp,
        
        UiOption.AllInOne,
        
        UiOption.ProcessDebug,
        UiOption.ProcessAttachConsole,
        UiOption.DriverDebug,
        UiOption.DriverAttachConsole,
        
        UiOption.Description,
        UiOption.SchedulingClass,

        UiOption.LogDirectory,
        UiOption.WorkingDirectory,
        UiOption.Jvm,
        
        UiOption.Classpath,
        UiOption.Environment,
       
        UiOption.DriverJvmArgs,
        UiOption.DriverDescriptorCR,
        UiOption.DriverDescriptorCROverrides,
        UiOption.DriverExceptionHandler,

        UiOption.ProcessJvmArgs,
        UiOption.ProcessMemorySize,
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
        //UiOption.ProcessGetMetaTimeMax,
        UiOption.ProcessInitializationTimeMax,

        UiOption.Specification,
        UiOption.WaitForCompletion,
        UiOption.CancelOnInterrupt,
        UiOption.ServiceDependency,
        UiOption.ClasspathOrder,
    };

    static UiOption[] opts_beta = new UiOption[] {
        UiOption.Help,
        UiOption.Debug, 
        UiOption.Timestamp,
        
        UiOption.AllInOne,
        
        UiOption.ProcessDebug,
        UiOption.ProcessAttachConsole,
        UiOption.DriverDebug,
        UiOption.DriverAttachConsole,
        
        UiOption.Description,
        UiOption.SchedulingClass,

        UiOption.LogDirectory,
        UiOption.WorkingDirectory,
        UiOption.Jvm,

        UiOption.Classpath,
        UiOption.Environment,
        
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
        //UiOption.ProcessGetMetaTimeMax,
        UiOption.ProcessInitializationTimeMax,

        UiOption.Specification,
        UiOption.WaitForCompletion,
        UiOption.CancelOnInterrupt,
        UiOption.CancelJobOnInterrupt,
        UiOption.ServiceDependency,
        UiOption.ClasspathOrder,
    };
    
    public static UiOption[] opts = opts_release;
    
    private AllInOneLauncher allInOneLauncher = null;
    
    /**
     * @param args List of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_SUBMIT">DUCC CLI reference.</a>
     */
    public DuccJobSubmit(ArrayList<String> args)
        throws Exception
    {
        this(args, null);
    }

    /**
     * @param args Array of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_SUBMIT">DUCC CLI reference.</a>
     */
    public DuccJobSubmit(String[] args)
        throws Exception
    {
        this(args, null);
    }

    /**
     * @param props Properties file of arguments, as described in the
     *      <a href="/doc/duccbook.html#DUCC_CLI_SUBMIT">DUCC CLI reference.</a>
     */
    public DuccJobSubmit(Properties props)
        throws Exception
    {
        this(props, null);
    }

    /**
     * This form of the constructor allows the API user to capture
     * messages, rather than directing them to stdout. 
     *
     * @param args List of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_SUBMIT">DUCC CLI reference.</a>
     * @param consoleCb If provided, messages are directed to it instead of
     *        stdout.
     */
    public DuccJobSubmit(ArrayList<String> args, IDuccCallback consoleCb)
        throws Exception
    {
        this(args.toArray(new String[args.size()]), consoleCb);
    }

    /**
     * This form of the constructor allows the API user to capture
     * messages, rather than directing them to stdout. 
     *
     * @param args Array of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_SUBMIT">DUCC CLI reference.</a>
     * @param consoleCb If provided, messages are directed to it instead of
     *        stdout.
     */
    public DuccJobSubmit(String[] args, IDuccCallback consoleCb)
        throws Exception
    {
        init();
        if(DuccUiUtilities.isSupportedBeta()) {
            opts = opts_beta;
        }
        init(this.getClass().getName(), opts, args, jobRequestProperties, or_host, or_port, "or", consoleCb, null);
        enrich_parameters_with_defaults(this, jobRequestProperties);
        if(isAllInOne()) {
            allInOneLauncher = new AllInOneLauncher(args, consoleCb);
        }
    }

    /**
     * This form of the constructor allows the API user to capture
     * messages, rather than directing them to stdout. 
     *
     * @param props Properties file contianing string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_SUBMIT">DUCC CLI reference.</a>
     * @param consoleCb If provided, messages are directed to it instead of
     *        stdout.
     */
    public DuccJobSubmit(Properties props, IDuccCallback consoleCb)
        throws Exception
    {
        for ( Object k : props.keySet() ) {      
            Object v = props.get(k);
            jobRequestProperties.put(k, v);
        }
        init();
        if(DuccUiUtilities.isSupportedBeta()) {
            opts = opts_beta;
        }
        init(this.getClass().getName(), opts, null, jobRequestProperties, or_host, or_port, "or", consoleCb, null);
        enrich_parameters_with_defaults(this, jobRequestProperties);
        if(isAllInOne()) {
            String[] args = mkArgs(props);
            allInOneLauncher = new AllInOneLauncher(args, consoleCb);
        }
    }
    
    protected void enrich_parameters_with_defaults(CliBase base, Properties props)
            throws Exception
    {
       	String pname = UiOption.SchedulingClass.pname();
        if(!props.containsKey(pname)) {
           	DuccSchedulerClasses duccSchedulerClasses = DuccSchedulerClasses.getInstance();
           	String scheduling_class = duccSchedulerClasses.getDefaultClassName();
           	if(scheduling_class != null) {
           		props.put(pname, scheduling_class);
           		String text = pname+"="+scheduling_class+" [default]";
           		base.message(text);
           	}
           	else {
           		throw new MissingArgumentException(pname);
           	}
        }
    }
    
    protected void transform_scheduling_class(CliBase base, Properties props)
            throws Exception
    {
    	 String scheduling_class = null;
         String text = null;
         String pname = UiOption.SchedulingClass.pname();
         DuccSchedulerClasses duccSchedulerClasses = DuccSchedulerClasses.getInstance();
         if(props.containsKey(pname)) {
             String user_scheduling_class = props.getProperty(pname);
             if(duccSchedulerClasses.isPreemptable(user_scheduling_class)) {
                 scheduling_class = duccSchedulerClasses.getDebugClassSpecificName(user_scheduling_class);
                 if(scheduling_class != null) {
                     text = pname+"="+scheduling_class+" [replacement, specific]";
                 }
                 else {
                     scheduling_class = duccSchedulerClasses.getDebugClassDefaultName();
                     text = pname+"="+scheduling_class+" [replacement, default]";
                 }
             }
             else {
                 scheduling_class = user_scheduling_class;
                 text = pname+"="+scheduling_class+" [original]";
             }
         }
         else {
             scheduling_class = duccSchedulerClasses.getDebugClassDefaultName();
             text = pname+"="+scheduling_class+" [default]";
         }
         if(scheduling_class != null) {
              props.setProperty(pname, scheduling_class);
              if(text != null) {
                  base.message(text);
              }
         }
    }
    
    private void set_debug_parms(Properties props, String key, int port)
    {
        String debug_jvmargs = "-Xdebug -Xrunjdwp:transport=dt_socket,address=" + host_address + ":" + port;
        String jvmargs = props.getProperty(key);
        if (jvmargs == null) {
            jvmargs = debug_jvmargs;
        } else {
            jvmargs += " " + debug_jvmargs;
        }
        props.put(key, jvmargs);
    }
    
    protected void enrich_parameters_for_debug(Properties props)
        throws Exception
    {
        String key_process = UiOption.ProcessJvmArgs.pname();
        String key_driver = UiOption.ProcessJvmArgs.pname();
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
                
                set_debug_parms(props, key_process, jp_debug_port);
                // For debugging, if the JP is being debugged, we have to force max processes to 1 & no restarts
                props.setProperty(UiOption.ProcessDeploymentsMax.pname(), "1");
                props.setProperty(UiOption.ProcessFailuresLimit.pname(), "1");
                
                // Alter scheduling class?
                transform_scheduling_class(this, props);
            }

            do_debug = UiOption.DriverDebug.pname();
            if ( props.containsKey(do_debug) ) {
                String jd_port_s = props.getProperty(do_debug);
                if ( jd_port_s == null ) {
                    throw new IllegalArgumentException("Missing port for " + do_debug);
                }
                jd_debug_port = Integer.parseInt(jd_port_s);
                set_debug_parms(props, key_driver, jd_debug_port);
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
            message(t.toString());
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
                message(UiOption.ProcessDeploymentsMax.pname(), ": requested[" + p_procs + "] adjusted to[" + a_procs + "]");
                properties.setProperty(UiOption.ProcessDeploymentsMax.pname(), a_procs);
                retVal = true;
            }
            else {
                long procs = Long.parseLong(p_procs);
                if( (procs * threads) > limit ) {
                    procs = limit / threads;
                    String a_procs = ""+procs;
                    message(UiOption.ProcessDeploymentsMax.pname(), ": requested["+p_procs+"] adjusted to["+a_procs + "]");
                    properties.setProperty(UiOption.ProcessDeploymentsMax.pname(), a_procs);
                    retVal = true;
                }
            }
        }
        catch(Throwable t) {
            message(t.toString());
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
            message(t.toString());
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

    //**********        
    
    /**
     * Execute collects the job parameters, does basic error and correctness checking, and sends
     * the job properties to the DUCC orchestrator for execution.
     *
     * @return True if the orchestrator accepts the job; false otherwise.
     */
    public boolean execute() throws Exception {
        if(allInOneLauncher != null) {
            return allInOneLauncher.execute();
        }
                    
        try {
            enrich_parameters_for_debug(jobRequestProperties);
        } catch (Exception e1) {
            message(e1.toString());
            return false;
        }
        
        boolean cp0 = jobRequestProperties.containsKey(UiOption.Classpath.pname());
        boolean cpd = jobRequestProperties.containsKey(UiOption.DriverClasspath.pname());
        boolean cpp = jobRequestProperties.containsKey(UiOption.ProcessClasspath.pname());
        
        if(cp0 && cpd) {
            throw new IllegalArgumentException("Conflict: cannot specify both "+UiOption.Classpath.pname()+" and "+UiOption.DriverClasspath.pname());
        }
        if(cp0 && cpp) {
            throw new IllegalArgumentException("Conflict: cannot specify both "+UiOption.Classpath.pname()+" and "+UiOption.ProcessClasspath.pname());
        }
        if(cp0) {
            // Nothing to do
        }
        else {
            if(cpd || cpp) {
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
            }
            else {
                /*
                 * employ default classpath if not specified
                 */
                String classpath = jobRequestProperties.getProperty(UiOption.Classpath.pname());
                if(classpath == null) {
                    classpath = System.getProperty("java.class.path");
                    jobRequestProperties.setProperty(UiOption.Classpath.pname(), classpath);
                }
            }
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
        boolean ev0 = jobRequestProperties.containsKey(UiOption.Environment.pname());
        boolean evd = jobRequestProperties.containsKey(UiOption.DriverEnvironment.pname());
        boolean evp = jobRequestProperties.containsKey(UiOption.ProcessEnvironment.pname());
        if(ev0 && evd) {
            throw new IllegalArgumentException("Conflict: cannot specify both "+UiOption.Environment.pname()+" and "+UiOption.DriverEnvironment.pname());
        }
        if(ev0 && evp) {
            throw new IllegalArgumentException("Conflict: cannot specify both "+UiOption.Environment.pname()+" and "+UiOption.ProcessEnvironment.pname());
        }
        if(ev0) {
            if (!DuccUiUtilities.ducc_environment(this, jobRequestProperties, UiOption.Environment.pname())) {
                return false;
            }
        }
        else {
            if(evd || evp) {
                if (!DuccUiUtilities.ducc_environment(this, jobRequestProperties, UiOption.DriverEnvironment.pname())) {
                    return false;
                }
                if (!DuccUiUtilities.ducc_environment(this, jobRequestProperties, UiOption.ProcessEnvironment.pname())) {
                    return false;
                }
            }
            else {
                if (!DuccUiUtilities.ducc_environment(this, jobRequestProperties, UiOption.Environment.pname())) {
                    return false;
                }
            }
        }
        
        /*
         * limit total number of threads
         */
        adjust_max_threads(jobRequestProperties);

        /*
         * adjust driver and process jvm args
         */
        boolean jad = jobRequestProperties.containsKey(UiOption.DriverJvmArgs.pname());
        boolean jap = jobRequestProperties.containsKey(UiOption.ProcessJvmArgs.pname());
        
        if(jad || jap) {
            adjust_specific_jvm_args(DuccPropertiesResolver.ducc_submit_driver_jvm_args, UiOption.DriverJvmArgs);
            adjust_specific_jvm_args(DuccPropertiesResolver.ducc_submit_process_jvm_args, UiOption.ProcessJvmArgs);
        }

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
            message("Job not submitted: " + e.getMessage());
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
    
    /**
     * Return appropriate rc when job has completed.
     * @return The exit code from the job.
     */
    public int getReturnCode() {
      if (allInOneLauncher != null) {
        return allInOneLauncher.getReturnCode();
      }
      return super.getReturnCode();
    }
    
    private boolean isAllInOne() {
        return jobRequestProperties.containsKey(UiOption.AllInOne.pname());
    }

    /**
     * Main method, as used by the executable jar or direct java invocation.
     * @param args arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_JOB">DUCC CLI reference.</a>
     */
    public static void main(String[] args) {
        try {
            DuccJobSubmit ds = new DuccJobSubmit(args, null);
            boolean rc = ds.execute();
            // If the return is 'true' then as best the API can tell, the submit worked
            if ( rc ) {                
                System.out.println("Job " + ds.getDuccId() + " submitted");
                int exit_code = ds.getReturnCode();       // after waiting if requested
                System.exit(exit_code);
            } else {
                System.out.println("Could not submit job");
                System.exit(1);
            }
        }
        catch(Exception e) {
            System.out.println("Cannot initialize: " + e);
            System.exit(1);
        }
    }
}
