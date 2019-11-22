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
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.IllegalConfigurationException;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;

/**
 * Submit a DUCC Managed Reservation that runs JED
 */

public class DuccJedSubmit extends CliBase {
  private static String dt = "Jed";

  private ServiceRequestProperties serviceRequestProperties;

  private UiOption[] opts = new UiOption[] { 
      UiOption.Help, 
      UiOption.Debug, 
      UiOption.Description, 
      UiOption.WorkingDirectory, // Must precede LogDirecory
      UiOption.LogDirectory,     // Must precede Environment
      UiOption.Environment, 
      UiOption.ProcessExecutableArgsRequired, 
      UiOption.ProcessMemorySize, 
      UiOption.SchedulingClass,
      UiOption.Specification, 
      };

  /**
   * @param args
   *          List of string arguments as described in the Command Line Interface section of the DuccBook
   * @throws Exception
   *           if request fails
   */
  public DuccJedSubmit(String[] args) throws Exception {
    this(args, null);
  }

  /**
   * @param args
   *          Array of string arguments as described in the Command Line Interface section of the DuccBook
   * @throws Exception
   *           if request fails
   */
  public DuccJedSubmit(ArrayList<String> args) throws Exception {
    this(args, null);
  }

  /**
   * @param props
   *          Properties file of arguments, as described in the Command Line Interface section of the DuccBook
   * @throws Exception
   *           if request fails
   */
  public DuccJedSubmit(Properties props) throws Exception {
    this(props, null);
  }

  /**
   * This form of the constructor allows the API user to capture messages, rather than directing them to stdout.
   *
   * @param args
   *          Array of string arguments as described in the Command Line Interface section of the DuccBook
   * @param consoleCb
   *          If provided, messages are directed to it instead of stdout.
   * @throws Exception
   *           if request fails
   */
  public DuccJedSubmit(String[] args, IDuccCallback consoleCb) throws Exception {
    serviceRequestProperties = new ServiceRequestProperties();
    init(this.getClass().getName(), opts, args, serviceRequestProperties, consoleCb);
  }

  /**
   * This form of the constructor allows the API user to capture messages, rather than directing them to stdout.
   *
   * @param args
   *          List of string arguments as described in the Command Line Interface section of the DuccBook
   * @param consoleCb
   *          If provided, messages are directed to it instead of stdout.
   * @throws Exception
   *           if request fails
   */
  public DuccJedSubmit(ArrayList<String> args, IDuccCallback consoleCb) throws Exception {
    String[] arg_array = args.toArray(new String[args.size()]);
    serviceRequestProperties = new ServiceRequestProperties();
    init(this.getClass().getName(), opts, arg_array, serviceRequestProperties, consoleCb);
  }

  /**
   * This form of the constructor allows the API user to capture messages, rather than directing them to stdout.
   *
   * @param props
   *          Properties file contianing string arguments as described in the Command Line Interface section of the DuccBook
   * @param consoleCb
   *          If provided, messages are directed to it instead of stdout.
   * @throws Exception
   *           if request fails
   */
  public DuccJedSubmit(Properties props, IDuccCallback consoleCb) throws Exception {
    serviceRequestProperties = new ServiceRequestProperties();
    init(this.getClass().getName(), opts, props, serviceRequestProperties, consoleCb);
  }

  /**
   * Execute collects the job parameters, does basic error and correctness checking, and sends the job properties to the DUCC orchestrator for
   * execution.
   *
   * @return True if the orchestrator accepts the job; false otherwise.
   * @throws Exception
   *           if request fails
   */
  public boolean execute() throws Exception {
    
    // Check if the experiments page is enabled
    if (DuccPropertiesResolver.get("ducc.experiments", "false").equalsIgnoreCase("false")) {
      throw new IllegalConfigurationException("JED experiments not enabled in ducc.properties");
    }
    
    // Create a copy to be saved later without these 3 "ducclet" properties required by DUCC
    ServiceRequestProperties userSpecifiedProperties = (ServiceRequestProperties) serviceRequestProperties.clone();
    serviceRequestProperties.setProperty(UiOption.ProcessPipelineCount.pname(), "1");
    serviceRequestProperties.setProperty(UiOption.ProcessDeploymentsMax.pname(), "1");
    serviceRequestProperties.setProperty(UiOption.ServiceTypeOther.pname(), "");
    
    // Create the AP properties for the JED call
    // Specify java, classpath, JED class, users's args
    // Explictly override any internal setting of the outputBaseDirectory
    // Augment the environment ... the OR checks for env-var DUCC_JED_DIR
    String javaHome = System.getProperty("java.home");
    serviceRequestProperties.setProperty(UiOption.ProcessExecutable.pname(), javaHome + "/bin/java");
    
    String jedArgs = "-cp " +
            ducc_home + "/lib/jed/*:" +
            ducc_home + "/lib/google-gson/*:" +
            ducc_home + "/apache-uima/lib/*:" +
            ducc_home + "/lib/uima-ducc-cli.jar " + 
            "com.ibm.watsonx.framework.jed.Driver ";
    String userArgs = serviceRequestProperties.getProperty(UiOption.ProcessExecutableArgsRequired.pname());
    String args = jedArgs + userArgs;
    serviceRequestProperties.setProperty(UiOption.ProcessExecutableArgsRequired.pname(), args);
    
    // Add the environment variable that tells the OR this is a JED AP
    // Also add DUCC_HOME so the JED properties can reference it
    String jedEnv = "DUCC_HOME="+ducc_home + " ";
    String userEnv = serviceRequestProperties.getProperty(UiOption.Environment.pname());
    if (userEnv != null) {
      jedEnv += userEnv;
    }
    serviceRequestProperties.setProperty(UiOption.Environment.pname(), jedEnv);
    
    // Create a description if none provided
    String desc = serviceRequestProperties.getProperty(UiOption.Description.pname());
    if (desc == null || desc.equals(UiOption.Description.deflt())) {
      serviceRequestProperties.setProperty(UiOption.Description.pname(), "JED: " + userArgs);
    }

    // Keep list of user provided properties for WS display: user vs. system
    for (Entry<Object, Object> entry : userSpecifiedProperties.entrySet()) {
      String key = (String) entry.getKey();
      serviceRequestProperties.addUserProvided(key);
    }

    // Note: context is provided for system event logger to disambiguate Service from ManagedReservation
    SubmitServiceDuccEvent ev = new SubmitServiceDuccEvent(serviceRequestProperties, DuccContext.ManagedReservation, CliVersion.getVersion());
    SubmitServiceReplyDuccEvent reply = null;

    try {
      reply = (SubmitServiceReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
    } finally {
      dispatcher.close();
    }

    /*
     * process reply
     */
    boolean rc = extractReply(reply);
    if (rc) {
      saveSpec(DuccUiConstants.managed_reservation_properties, userSpecifiedProperties);
      startMonitors(true, DuccContext.ManagedReservation); // starts conditionally, based on job spec and console listener present
    }

    return rc;
  }

  /**
   * Main method, as used by the executable jar or direct java invocation.
   * 
   * @param args
   *          arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_PROCESS_SUBMIT">DUCC CLI reference.</a>
   */
  public static void main(String[] args) {
    int code = 1; // Assume the worst
    try {
      // Instantiate the object with args similar to the CLI, or a pre-built properties file
      DuccJedSubmit ds = new DuccJedSubmit(args);

      // Run the API. If process_attach_console was specified in the args, a console listener is
      // started but this call does NOT block on it.
      boolean rc = ds.execute();

      // If the return is 'true' then as best the API can tell, the submit worked
      if (rc) {
        System.out.println(dt + " " + ds.getDuccId() + " submitted.");
        code = ds.getReturnCode();
      } else {
        System.out.println(dt + " Could not submit ");
      }
    } catch (Throwable e) {
      System.out.println(dt + " Cannot initialize: " + e);
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
    } finally {
      // Set the process exit code
      System.exit(code);
    }
  }

}
