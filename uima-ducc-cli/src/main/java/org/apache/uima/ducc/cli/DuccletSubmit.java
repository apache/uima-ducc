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
import java.io.FileInputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;


/**
 * Submit a DUCC service
 */

public class DuccletSubmit 
    extends CliBase
{
    
    static String or_port = "ducc.orchestrator.http.port";
    static String or_host = "ducc.orchestrator.node";
    boolean console_attach = false;

    int pid = -1;
    ServiceRequestProperties serviceRequestProperties;

    public DuccletSubmit(String[] args)
        throws Exception
    {
        serviceRequestProperties = new ServiceRequestProperties();        
        init(args, serviceRequestProperties, or_host, or_port, "or");
    }
        
    public DuccletSubmit(Properties props) 
        throws Exception
    {
        serviceRequestProperties = new ServiceRequestProperties();
        // TODO - can we get OR to just use properties and not these specialized classes?
        //        Until then, we need to pass in the right kind of props, sigh.
        for ( Object k : props.keySet() ) {      
            Object v = props.get(k);
            serviceRequestProperties.put(k, v);
        }
        init(null, props, or_host, or_port, "or");
    }
        
    @SuppressWarnings("static-access")
    void addOptions(Options options) {
        options.addOption(OptionBuilder
                .withDescription(DuccUiConstants.desc_help).hasArg(false)
                .withLongOpt(DuccUiConstants.name_help).create());

        options.addOption(OptionBuilder
                .withDescription(DuccUiConstants.desc_debug).hasArg(false)
                .withLongOpt(DuccUiConstants.name_debug).create());

        options.addOption(OptionBuilder
                .withArgName(DuccUiConstants.parm_description)
                .withDescription(makeDesc(DuccUiConstants.desc_description,DuccUiConstants.exmp_description)).hasArg()
                .withLongOpt(DuccUiConstants.name_description).create());

        options.addOption(OptionBuilder
                .withArgName(DuccUiConstants.parm_scheduling_class)
                .withDescription(makeDesc(DuccUiConstants.desc_scheduling_class,DuccUiConstants.exmp_scheduling_class)).hasArg()
                .withLongOpt(DuccUiConstants.name_scheduling_class).create());

        options.addOption(OptionBuilder
                .withArgName(DuccUiConstants.parm_log_directory)
                .withDescription(makeDesc(DuccUiConstants.desc_log_directory,DuccUiConstants.exmp_log_directory)).hasArg()
                .withLongOpt(DuccUiConstants.name_log_directory).create());

        options.addOption(OptionBuilder
                .withArgName(DuccUiConstants.parm_working_directory)
                .withDescription(makeDesc(DuccUiConstants.desc_working_directory,DuccUiConstants.exmp_working_directory)).hasArg()
                .withLongOpt(DuccUiConstants.name_working_directory).create());

        options.addOption(OptionBuilder
                .withArgName(DuccUiConstants.parm_process_environment)
                .withDescription(makeDesc(DuccUiConstants.desc_process_environment,DuccUiConstants.exmp_process_environment)).hasArg()
                .withLongOpt(DuccUiConstants.name_process_environment).create());

        options.addOption(OptionBuilder
                .withArgName(DuccUiConstants.parm_process_memory_size)
                .withDescription(makeDesc(DuccUiConstants.desc_process_memory_size,DuccUiConstants.exmp_process_memory_size)).hasArg()
                .withLongOpt(DuccUiConstants.name_process_memory_size).create());

        options.addOption(OptionBuilder
                .withArgName(DuccUiConstants.parm_process_failures_limit)
                .withDescription(makeDesc(DuccUiConstants.desc_process_failures_limit,DuccUiConstants.exmp_process_failures_limit)).hasArg()
                .withLongOpt(DuccUiConstants.name_process_failures_limit).create());

		options.addOption(OptionBuilder
                          .withArgName    (DuccUiConstants.parm_specification)
                          .withDescription(DuccUiConstants.desc_specification)
                          .hasArg         (true)
                          .withLongOpt    (DuccUiConstants.name_specification)
                          .create         ()
                          );

        options.addOption(OptionBuilder
                          .withDescription(DuccUiConstants.desc_process_attach_console)
                          .hasOptionalArg ()
                          .withLongOpt    (DuccUiConstants.name_process_attach_console)
                          .create         ()
                          );

        options.addOption(OptionBuilder
                          .withArgName    (DuccUiConstants.parm_process_executable)
                          .withDescription(makeDesc(DuccUiConstants.desc_process_executable,DuccUiConstants.exmp_process_executable)).hasArg()
                          .withLongOpt    (DuccUiConstants.name_process_executable)
                          .hasArg         (true)
                          .create         ()
                          );

        options.addOption(OptionBuilder
                          .withArgName    (DuccUiConstants.parm_process_executable_args)
                          .withDescription(makeDesc(DuccUiConstants.desc_process_executable_args,DuccUiConstants.exmp_process_executable_args))
                          .hasArgs        ()
                          .withLongOpt    (DuccUiConstants.name_process_executable_args)
                          .create         ()
                          );

    }
                
    public boolean execute() throws Exception 
    {
        /*
         * parser is not thread safe?
         */
        synchronized(DuccUi.class) {

            /*
             * require DUCC_HOME 
             */
            String ducc_home_key = "DUCC_HOME";
            String ducc_home = System.getenv(ducc_home_key);
            if(ducc_home == null) {
                addError("missing required environment variable: " + ducc_home_key);
                return false;
            }
                        
            /*
             * marshal user
             */
            String user = DuccUiUtilities.getUser();
            serviceRequestProperties.setProperty(ServiceSpecificationProperties.key_user, user);
            String property = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_signature_required);
            if(property != null) {
                String signatureRequiredProperty = property.trim().toLowerCase();
                if(signatureRequiredProperty.equals("on")) {
                    Crypto crypto = new Crypto(System.getProperty("user.home"));
                    byte[] cypheredMessage = crypto.encrypt(user);
                    serviceRequestProperties.put(ServiceSpecificationProperties.key_signature, cypheredMessage);
                }
            }
            /*
             * marshal command line options into properties
             */
            Option[] optionList = commandLine.getOptions();
            // pass 1
            for (int i=0; i<optionList.length; i++) {
                Option option = optionList[i];
                String name = option.getLongOpt();
                if(name.equals(SpecificationProperties.key_specification)) {
                    File file = new File(option.getValue());
                    FileInputStream fis = new FileInputStream(file);
                    serviceRequestProperties.load(fis);
                }
            }

            DuccUiUtilities.trimProperties(serviceRequestProperties);
            // pass 2
            for (int i=0; i<optionList.length; i++) {
                Option option = optionList[i];
                String name = option.getLongOpt();

                String value = null;
                if ( option.hasArgs() ) {        // if multiple args, make into blank-delimited string for the props file
                    String[] arglist = commandLine.getOptionValues(name);
                    int len = arglist.length;
                    StringBuffer sb = new StringBuffer();
                    for ( int ii = 0; ii < len; ii++ ) {
                        String a = arglist[ii].trim();
                        if ( a.equals("") ) continue;
                        sb.append(a);
                        if ( ii < (len-1) ) {
                            sb.append(", ");
                        }
                    }
                    value = sb.toString();
                } else {
                    value = option.getValue();
                }

                if(value == null) {
                    value = "";
                }

                name = trimmer(name);
                value = trimmer(value);
                setProperty(name, value);
            }
        }
        
        /*
         * make sure the logdir is actually legal.
         */
        String log_directory = getLogDirectory(serviceRequestProperties);
        if ( log_directory == null ) {    // on failure, the internal errors are already set
            return false;
        }

        // tack on "services" or "processes" to complete logging directory
        if(log_directory.endsWith(File.separator)) {
            log_directory = log_directory + "processes";
        } else {
            log_directory = log_directory + File.separator + "processes";
        }
        serviceRequestProperties.setProperty("log_directory", log_directory);

        /*
         * employ default working directory if not specified
         */
        String working_directory = serviceRequestProperties.getProperty(ServiceRequestProperties.key_working_directory);
        if(working_directory == null) {
            working_directory = System.getProperty("user.dir");
            serviceRequestProperties.setProperty(ServiceRequestProperties.key_working_directory,working_directory);
        }

        // These must be enforce so OR doesn't complain
        serviceRequestProperties.setProperty(DuccUiConstants.name_process_thread_count, "1");
        serviceRequestProperties.setProperty(DuccUiConstants.name_process_deployments_max, "1");     

        serviceRequestProperties.put(ServiceRequestProperties.key_service_type_other, "");
        
        if(serviceRequestProperties.containsKey(DuccUiConstants.name_debug)) {
            serviceRequestProperties.dump();
        }

        /*
         * set DUCC_LD_LIBRARY_PATH in process environment
         */
        adjustLdLibraryPath(serviceRequestProperties, ServiceRequestProperties.key_process_environment);

        /*
         * identify invoker
         */
        serviceRequestProperties.setProperty(ServiceRequestProperties.key_submitter_pid_at_host, ManagementFactory.getRuntimeMXBean().getName());

        console_attach =
            serviceRequestProperties.containsKey(DuccUiConstants.name_process_attach_console);

        if ( console_attach ) {
            try {
                startConsoleListener();            
            } catch ( Throwable t ) {
                throw new IllegalStateException("Cannot start console listener.  Reason:" + t.getMessage());
            }
            set_console_port(serviceRequestProperties, DuccUiConstants.name_process_environment);
		}

        SubmitServiceDuccEvent ev = new SubmitServiceDuccEvent(serviceRequestProperties);
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
        boolean retval = true;
        Properties properties = reply.getProperties();
        @SuppressWarnings("unchecked")
		ArrayList<String> or_warnings = (ArrayList<String>) properties.get(ServiceSpecificationProperties.key_submit_warnings);
        if (or_warnings != null) {
            addWarnings(or_warnings);
        }

        @SuppressWarnings("unchecked")
        ArrayList<String> or_errors = (ArrayList<String>) properties.get(ServiceSpecificationProperties.key_submit_errors);
        if(or_errors != null) {
            addErrors(or_errors);
            retval = false;
        }

        if ( retval ) {
            String pid = reply.getProperties().getProperty(ServiceRequestProperties.key_id);
            if (pid == null ) {
                retval = false;
            } else {
                friendlyId = Long.parseLong(pid);
                if ( friendlyId < 0 ) {
                    retval = false;
                } else {
                    saveSpec(pid, "process.properties", serviceRequestProperties);
                }
            }
        }

        return retval;
    }
        
    public static void main(String[] args) 
    {
        try {
            DuccletSubmit ds = new DuccletSubmit(args);            
            boolean rc = ds.execute();

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

            if ( rc ) {
            	System.out.println("Process " + ds.getDuccId() + " submitted.");

                if ( ds.isConsoleAttached() ) {
                    ds.waitForCompletion();
                }

            	System.exit(0);
            } else {
                System.out.println("Could not submit process");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("Cannot initialize: " + e.getMessage());
            System.exit(1);
        }
    }
    
}
