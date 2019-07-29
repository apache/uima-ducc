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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.uima.ducc.cli.aio.AllInOneLauncher;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.Utils;
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

    public static UiOption[] opts = new UiOption[] {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Timestamp,

        UiOption.AllInOne,

        UiOption.AttachConsole,
        UiOption.ProcessDebug,
        UiOption.DriverDebug,

        UiOption.Description,
        UiOption.SchedulingClass,

        UiOption.WorkingDirectory,   // Must precede LogDirecory
        UiOption.LogDirectory,       // Must precede Environment
        UiOption.Jvm,

        UiOption.Classpath,
        UiOption.Environment,

        UiOption.DriverJvmArgs,
        UiOption.DriverDescriptorCR,
        UiOption.DriverDescriptorCROverrides,
        UiOption.DriverExceptionHandler,
        UiOption.DriverExceptionHandlerArguments,

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
        UiOption.ProcessPipelineCount,
        UiOption.ProcessErrorWindowThreshold,
        UiOption.ProcessErrorWindowSize,
        UiOption.ProcessPerItemTimeMax,
        UiOption.ProcessInitializationTimeMax,

        UiOption.Specification,
        UiOption.SuppressConsoleLog,
        UiOption.WaitForCompletion,
        UiOption.CancelOnInterrupt,
        UiOption.ServiceDependency,
    };

    private AllInOneLauncher allInOneLauncher = null;

    /**
     * Submit a job
     * 
     * @param args Array of string arguments as described in the
     * Command Line Interface section of the DuccBook
     * @throws Exception if request fails
     */
    public DuccJobSubmit(String[] args)
        throws Exception
    {
        this(args, null);
    }

    /**
     * Submit a job
     * 
     * @param args List of string arguments as described in the
     * Command Line Interface section of the DuccBook
     * @throws Exception if request fails
     */
    public DuccJobSubmit(ArrayList<String> args)
        throws Exception
    {
        this(args, null);
    }


    /**
     * Submit a job
     * 
     * @param props Properties file of arguments, as described in the
     * Command Line Interface section of the DuccBook
     * @throws Exception if request fails
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
     * @throws Exception if request fails
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
     * @throws Exception if request fails
     */
    public DuccJobSubmit(String[] args, IDuccCallback consoleCb)
        throws Exception
    {
        init (this.getClass().getName(), opts, args, jobRequestProperties, consoleCb);
        check_descriptor_options();
        check_error_window_definition();
        if (isAllInOne()) {
            allInOneLauncher = new AllInOneLauncher(userSpecifiedProperties, consoleCb);  // Pass the already fixed-up user properties
        }
    }

    /**
     * This form of the constructor allows the API user to capture
     * messages, rather than directing them to stdout.
     *
     * @param props Properties file containing string arguments as described in the
     *      <a href="/doc/duccbook.html#DUCC_CLI_SUBMIT">DUCC CLI reference.</a>
     * @param consoleCb If provided, messages are directed to it instead of
     *        stdout.
     * @throws Exception if request fails
     */
    public DuccJobSubmit(Properties props, IDuccCallback consoleCb)
        throws Exception
    {
        init (this.getClass().getName(), opts, props, jobRequestProperties, consoleCb);
        
        check_descriptor_options();
        check_error_window_definition();
        if (isAllInOne()) {
            allInOneLauncher = new AllInOneLauncher(userSpecifiedProperties, consoleCb);  // Pass the already fixed-up user properties
        }
    }
    private void check_error_window_definition() throws IllegalArgumentException {
    	int windowSize=0;
    	int maxThreshold=0;
    	
		boolean errorWindowSizeDefined = jobRequestProperties.containsKey(UiOption.ProcessErrorWindowSize.pname());
		boolean errorWindowThresholdDefined = jobRequestProperties.containsKey(UiOption.ProcessErrorWindowThreshold.pname());

		if ( errorWindowSizeDefined ) {
			try {
				windowSize = jobRequestProperties.getIntProperty(UiOption.ProcessErrorWindowSize.pname());
			} catch( NumberFormatException e) {
				throw new IllegalArgumentException("process_error_window_size must be a postive number - you've provided:"+
						jobRequestProperties.getIntProperty(UiOption.ProcessErrorWindowSize.pname()));
			} 
		}
		if ( errorWindowThresholdDefined ) {
			try {
				maxThreshold = jobRequestProperties.getIntProperty(UiOption.ProcessErrorWindowThreshold.pname());
			} catch( NumberFormatException e) {
				throw new IllegalArgumentException("process_error_window_threshold must be a postive number - you've provided:"+
						jobRequestProperties.getIntProperty(UiOption.ProcessErrorWindowThreshold.pname()));
			}
		}
		if ( maxThreshold > windowSize   ) {
			throw new IllegalArgumentException("process_error_window_threshold must be smaller/equal than/to process_error_window_size - you've provided:"+
					" process_error_window_threshold="+jobRequestProperties.getIntProperty(UiOption.ProcessErrorWindowThreshold.pname()) +
					" process_error_window_size="+jobRequestProperties.getIntProperty(UiOption.ProcessErrorWindowSize.pname()));
			
		}
    }

  private void check_descriptor_options() {
		boolean isDDjob = jobRequestProperties.containsKey(UiOption.ProcessDD.pname());
		boolean isPPjob = jobRequestProperties.containsKey(UiOption.ProcessDescriptorCM.pname())
				|| jobRequestProperties.containsKey(UiOption.ProcessDescriptorAE.pname())
				|| jobRequestProperties.containsKey(UiOption.ProcessDescriptorCC.pname());
		if (isDDjob && isPPjob) {
			throw new IllegalArgumentException("--process_descriptor_DD is mutually exclusive with the AE, CC, & CM descriptor options");
		}
		if (!isDDjob && !isPPjob) {
			throw new IllegalArgumentException("Missing --process_descriptor_xx option .. DD or at least one of AE, CC, CM required");
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
                // For debugging, if the JP is being debugged, we have to force max processes to 1 & no restarts
                props.setProperty(UiOption.ProcessDeploymentsMax.pname(), "1");
                props.setProperty(UiOption.ProcessFailuresLimit.pname(), "1");

                // Scheduling class now checked in OR's Validate
            }

            do_debug = UiOption.DriverDebug.pname();
            if ( props.containsKey(do_debug) ) {
                String jd_port_s = props.getProperty(do_debug);
                if ( jd_port_s == null ) {
                    throw new IllegalArgumentException("Missing port for " + do_debug);
                }
                jd_debug_port = Integer.parseInt(jd_port_s);
                set_debug_parms(props, UiOption.DriverJvmArgs.pname(), jd_debug_port);
            }

            if ( jp_debug_port == jd_debug_port ) {
                throw new IllegalArgumentException("Process and Driver debug ports must differ.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid debug port (not numeric)");
        }

    }
	  private URLClassLoader newClassLoader(String[] classPathElements) throws IOException  {
		    ArrayList<URL> urlList = new ArrayList<URL>(classPathElements.length);
		    for (String element : classPathElements) {
		      if (element.endsWith("*")) {
		        File dir = new File(element.substring(0, element.length() - 1));
		        File[] files = dir.listFiles();   // Will be null if missing or not a dir
		        if (files != null) {
		          for (File f : files) {
		            if (f.getName().endsWith(".jar")) {
		              urlList.add(f.getCanonicalFile().toURI().toURL());
		            }
		          }
		        }
		      } else {
		        File f = new File(element);
		        if (f.exists()) {
		          urlList.add(f.getCanonicalFile().toURI().toURL());
		        }
		      }
		    }
		    URL[] urls = new URL[urlList.size()];
		    return new URLClassLoader(urlList.toArray(urls), ClassLoader.getSystemClassLoader().getParent());
		  }
    //**********
	private void modifyClasspathBasedOnUimaVersion(String classpathKey) throws Exception {
        String duccHomePath = Utils.findDuccHome();
        
        String classpath = jobRequestProperties.getProperty(classpathKey);
		String[] jars = classpath.split(":");
		URLClassLoader clsLoader = newClassLoader(jars);
		Class<?> cls = clsLoader.loadClass("org.apache.uima.impl.UimaVersion");
		Method majorVersionMethod = cls.getMethod("getMajorVersion");
		short majorVersion = (short)majorVersionMethod.invoke(null);
		
		
        if ( !duccHomePath.trim().endsWith("/") ) {
			duccHomePath = duccHomePath.concat("/");
		}
		String workItemJarDir = duccHomePath+"lib/uima-ducc/workitem/uima-ducc-workitem-";
		if ( majorVersion < 3 ) {
			classpath = workItemJarDir+"v2.jar:"+classpath;
		} else if ( majorVersion >= 3 ) {
			classpath = workItemJarDir+"v3.jar:"+classpath;
		} else {
			throw new DuccRuntimeException("Unknown version of UIMA - majorVersion="+majorVersion);
		}
		jobRequestProperties.setProperty(classpathKey,classpath);
	}

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
        // Defaults now provided in init()
        try {
            enrich_parameters_for_debug(jobRequestProperties);
        } catch (Exception e1) {
            message(e1.toString());
            Throwable t = e1;
            while ((t = t.getCause()) != null) {
                message("  ... " + t);
            }
            if (jobRequestProperties.containsKey(UiOption.Debug.pname())) {
                e1.printStackTrace();
            }
            return false;
        }

        /*
         * Set default classpath if not specified
         */
        String key_cp = UiOption.Classpath.pname();
        if (!jobRequestProperties.containsKey(key_cp)) {
            jobRequestProperties.setProperty(key_cp, System.getProperty("java.class.path"));
        }
        // using user classpath determine which uima version is being used and 
        // add to the front a corresponding version of uima-ducc-workitem-v<N>.jar
        modifyClasspathBasedOnUimaVersion(key_cp);
        
        if (jobRequestProperties.containsKey(UiOption.Debug.pname())) {
            jobRequestProperties.dump();
        }

        /*
         * resolve ${defaultBrokerURL} in service dependencies - must fail if resolution needed but can't resolve
         */
        if ( ! check_service_dependencies(null) ) {
            return false;
        }

        // Warn if Xmx value is too large and may cause swapping
        check_heap_size(UiOption.ProcessJvmArgs.pname());
        
        /*
         * keep list of user provided properties for WS display: user vs. system
         */
        for(Entry<Object, Object> entry : userSpecifiedProperties.entrySet()) {
        	String key = (String) entry.getKey();
        	jobRequestProperties.addUserProvided(key);
        }
        
        SubmitJobDuccEvent      submitJobDuccEvent      = new SubmitJobDuccEvent(jobRequestProperties, CliVersion.getVersion());
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

    public long getDuccId() {
        if (allInOneLauncher != null) {
            return allInOneLauncher.getDuccId();
        }
        return super.getDuccId();
    }

    private boolean isAllInOne() {
        return jobRequestProperties.containsKey(UiOption.AllInOne.pname());
    }

    /**
     * Main method, as used by the executable jar or direct java invocation.
     * @param args arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SUBMIT">DUCC CLI reference.</a>
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
        catch(Throwable e) {
            System.out.println("Cannot initialize: " + e);
            Throwable t = e;
            while ((t = t.getCause()) != null) {
                System.out.println("  ... " + t);
            }
            for (String arg : args) {
                if (arg.equals("--debug")) {
                    e.printStackTrace();
                    break;
                }
            }
            System.exit(1);
        }
    }
}
