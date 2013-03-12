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

import org.apache.uima.ducc.common.jd.plugin.IJdProcessExceptionHandler;
import org.apache.uima.ducc.transport.event.cli.JobReplyProperties;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.JobSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;

/**
 * These are the constants supported by the 'not-using-a-props-file' form of registration in DuccServiceApi.
 * 
 * There are here so that hopefully we can avoid touching anything in common or transport when updating
 * the registration parameters.
 */

public interface IUiOptions
{
    //
    // decode() mean convert the enum into the string the user uses
    // encode() means take the user's string and turn it into th enum
    // description() is a short description of the option for the commons cli parser
    // argname()     is a name for the argument for the usage() part of cli parser
    //



    public enum UiOption
    {
        Activate   { 
            public String pname()       { return "activate"; } 
            public String argname()     { return null; }
            public String description() { return "If present, apply current service updates to the running instances.."; } 
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },

        Autostart   { 
            public String pname()      { return "autostart"; } 
            public String argname()     { return "boolean: true or false"; } 
            public String description() { return "If True, start the service when DUCC starts."; } 
            public String example()     { return null; }
            public String deflt()       { return "false"; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },

        CancelJobOnInterrupt { 
            public String pname()       { return "cancel_job_on_interrupt"; }
            public String argname()     { return null; }
            public String description() { return "Cancel job on interrupt (Ctrl-C); only possible when --" + WaitForCompletion.pname() +" is also specified."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ClasspathOrder { 
            public String pname()       { return JobSpecificationProperties.key_classpath_order; }
            public String argname()     { return ClasspathOrderParms.UserBeforeDucc.name() + " or " + ClasspathOrderParms.DuccBeforeUser.name(); }
            public String description() { return "Specify user-supplied classpath before DUCC-supplied classpath, or the reverse."; }
            public String example()     { return null; }
            public String deflt()       { return ClasspathOrderParms.UserBeforeDucc.name(); }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Debug { 
            public String pname()       { return "debug"; }
            public String argname()     { return null; }
            public String description() { return "Enable CLI Debugging messages."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        DjPid { 
            public String pname()       { return JobRequestProperties.key_dpid; }
            public String argname()     { return "number"; }
            public String description() { return "DUCC Process Id.  If specified only this DUCC process will be canceled.  If not specified, then entire job will be canceled.";}
            public String example()     { return "22"; }
            public String deflt()       { return null; }
            public String label()       { return "Ducc Process Id"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        DriverDebug { 
            public String pname()       { return "driver_debug"; }
            public String argname()     { return "debugger-port-number"; }
            public String description() { return "Append JVM debug flags to the jvm arguments to start the JobDriver in remote debug mode."; }
            public String example()     { return "driver_debug 8001"; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessDebug"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Description { 
            public String pname()       { return JobSpecificationProperties.key_description; }
            public String argname()     { return "string"; }
            public String description() { return "Description of the run."; }
            public String example()     { return "--description My excellent job!"; }
            public String deflt()       { return null; }
            public String label()       { return "Description"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        DriverAttachConsole { 
            public String pname()       { return "driver_attach_console"; }
            public String argname()     { return null; }
            public String description() { return "If specified, redirect remote job driver stdout and stderr to the local submitting console."; }
            public String example()     { return null; }
            public String deflt()       { return "Do not redirect the console"; }
            public String label()       { return "DriverAttachConsole"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        DriverClasspath { 
            public String pname()       { return JobSpecificationProperties.key_driver_classpath; }
            public String description() { return "Classpath for the Job driver. Default is current classpath."; }
            public String argname()     { return "java classpath"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return "DriverClassPath"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        DriverDescriptorCR { 
            public String pname()       { return JobSpecificationProperties.key_driver_descriptor_CR; }
            public String description() { return "Driver (collection reader) descriptor."; }
            public String argname()     { return "descriptor xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyCR.xml"; }
            public String deflt()       { return null; }
            public String label()       { return "DriverDescriptorCR"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return true; }
        },            

        DriverDescriptorCROverrides { 
            public String pname()       { return JobSpecificationProperties.key_driver_descriptor_CR_overrides; }
            public String argname()     { return "list of overrides"; }
            public String description() { return "Driver Collection Reader configuration parameter name/value pair overrides. Parameters must already be defined in the CR descriptor."; }
            public String example()     { return "name1=value1,name2=\"value2a value2b value2c\",name3=value3..."; }
            public String deflt()       { return null; }
            public String label()       { return "DriverDescriptorCROverrides"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        DriverEnvironment { 
            public String pname()       { return JobSpecificationProperties.key_driver_environment; }
            public String argname()     { return "env vars"; }
            public String description() { return "Blank-delimeted list of environment variables."; }
            public String example()     { return "\"TERM=xterm DISPLAY=me.org.net:1.0\""; }
            public String deflt()       { return null; }
            public String label()       { return "DriverEnvironment"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        DriverExceptionHandler { 
            public String pname()       { return JobSpecificationProperties.key_driver_exception_handler; }
            public String description() { return "Driver Exception handler class.  Must implement "+IJdProcessExceptionHandler.class.getName(); }
            public String argname()     { return "path.classname"; }
            public String example()     { return "org.apache.uima.ducc.myProject.MyDriverExceptionHandler"; }
            public String deflt()       { return "org.apache.uima.ducc.common.jd.plugin.JdProcessExceptionHandler"; }
            public String label()       { return "DriverExceptionHandler"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        DriverJvmArgs { 
            public String pname()       { return JobSpecificationProperties.key_driver_jvm_args; }
            public String argname()     { return "jvm arguments"; }
            public String description() { return "Blank-delimeted list of JVM Arguments passed to the job driver."; }
            public String example()     { return "-Xmx100M -DMYVAR=foo"; }
            public String deflt()       { return null; }
            public String label()       { return "DriverJvmArgs"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Help { 
            public String pname()       { return "help"; }
            public String argname()     { return null; }
            public String description() { return "Print this help message"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Instances   { 
            public String pname()       { return "instances"; } 
            public String argname()     { return "integer"; } 
            public String description() { return "Number of service instances to start or stop."; } 
            public String example()     { return null; }
            public String deflt()       { return "1"; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },

        JobId { 
            public String pname()       { return JobRequestProperties.key_id; }
            public String argname()     { return "string"; }
            public String description() { return "The id of the job"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return true; }
        },            

        Jvm { 
            public String pname()       { return JobSpecificationProperties.key_jvm; }
            public String argname()     { return "path-name-to-java"; }
            public String description() { return "The jvm to use.  Must be a full path to the 'java' executable.  Default is\n   the jvm that DUCC is using."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return "Jvm"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        LogDirectory { 
            public String pname()       { return JobSpecificationProperties.key_log_directory; }
            public String argname()     { return "path"; }
            public String description() { return "The directory where logs are written.  Default: $HOME/ducc/logs"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return "LogDirectory"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Message { 
            public String pname()       { return JobReplyProperties.key_message; }
            public String argname()     { return "string"; }
            public String description() { return "Orchestrator response string - internally generated."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Modify    { 
             public String pname()      { return "modify"; } 
             public String argname()     { return "modify-parameters" ; } 
             public String description() { return "Modify meta properties for a registered service." ; } 
             public String example()     { return null; }
             public String deflt()       { return null; }
             public String label()       { return name(); }
             public boolean multiargs()  { return false; }
             public boolean required()   { return true; }
        },

        NumberOfInstances { 
            public String pname()       { return ReservationSpecificationProperties.key_number_of_instances; }
            public String argname()     { return "integer"; }
            public String description() { return "Number of instances to reserve"; }
            public String example()     { return "1"; }
            public String deflt()       { return "1"; }
            public String label()       { return "Number of instances"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Register    { 
            public String pname()       { return "register"; } 
            public String argname()     { return null; } 
            public String description() { return "Register a service."; } 
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },

        ServicePingClass { 
            public String pname()       { return "service_ping_class"; }
            public String argname()     { return "classname"; }
            public String description() { return "Class to ping ervice, must extend AServicePing.java"; }
            public String example()     { return "org.bob.Pinger"; }
            public String deflt()       { return ""; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ServicePingClasspath { 
            public String pname()       { return "service_ping_classpath"; }
            public String argname()     { return "classpath"; }
            public String description() { return "Classpath containing service_custom_ping class and dependencies."; }
            public String example()     { return "Bob.jar"; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ServicePingJvmArgs { 
            public String pname()       { return "service_ping_jvm_args"; }
            public String argname()     { return "java-system-property-assignments"; }
            public String description() { return "-D jvm system property assignments to pass to jvm"; }
            public String example()     { return "-DxmX=3"; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ServicePingTimeout { 
            public String pname()       { return "service_ping_timeout"; }
            public String argname()     { return "time-in-ms"; }
            public String description() { return "Time in milliseconds to wait for a ping to the service."; }
            public String example()     { return "1000"; }
            public String deflt()       { return "500"; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ServicePingDoLog { 
            public String pname()       { return "service_ping_dolog"; }
            public String argname()     { return "boolean"; }
            public String description() { return "If specified, log the pinger, else suppress the log."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ServiceTypeCustom { 
            public String pname()       { return ServiceRequestProperties.key_service_type_custom; }
            public String argname()     { return null; }
            public String description() { return "Service type - internally generated"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ServiceTypeOther { 
            public String pname()       { return ServiceRequestProperties.key_service_type_other; }
            public String argname()     { return null; }
            public String description() { return "Service type - internally generated"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ServiceTypeUima { 
            public String pname()       { return ServiceRequestProperties.key_service_type_uima; }
            public String argname()     { return null; }
            public String description() { return "Service type - internally generated"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Start       { 
            public String pname()       { return "start"; } 
            public String description() { return "Start a registered service." ; } 
            public String argname()     { return "service-id-or-endpoint" ; } 
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },

        Stop        { 
            public String pname()      { return "stop"; } 
            public String description() { return "Stop a registered service." ; } 
            public String argname()     { return "service-id-or-endpoint [--instances number-to-stop]" ; } 
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },

        SubmitPid { 
            // generated
            public String pname()       { return JobRequestProperties.key_submitter_pid_at_host; }
            public String argname()     { return null; }
            public String description() { return null; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessAttachConsole { 
            public String pname()       { return "process_attach_console"; }
            public String argname()     { return null; }
            public String description() { return "If specified, redirect remote process stdout and stderr to the local submitting console."; }
            public String example()     { return null; }
            public String deflt()       { return "Do not redirect the console"; }
            public String label()       { return "ProcessAttachConsole"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessDebug { 
            public String pname()       { return "process_debug"; }
            public String argname()     { return "debugger-port-number"; }
            public String description() { return "Append JVM debug flags to the jvm arguments to start the Job Process in remobe debug mode."; }
            public String example()     { return "--process_debug 8000"; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessDebug"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessClasspath { 
            public String pname()       { return JobSpecificationProperties.key_process_classpath; }
            public String description() { return "Classpath for each job process. Default is current classpath."; }
            public String argname()     { return "java classpath"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return "processClassPath"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessDescriptorAE { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_AE; }
            public String description() { return "Process Analysis Enginefor aggregate."; }
            public String argname()     { return "descriptor xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyAE.xml"; }
            public String deflt()       { return null; }
            public String label()       { return "DriverDescriptorAE"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessDescriptorAEOverrides { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_AE_overrides; }
            public String argname()     { return "list of overrides"; }
            public String description() { return "Process Analysis Engine configuration parameter name/value pair overrides. Parameters must already be defined in the AE descriptor."; }
            public String example()     { return "name1=value1,name2=\"value2a value2b value2c\",name3=value3..."; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessDescriptorAEOverrides"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        ProcessDescriptorCC { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_CC; }
            public String description() { return "Process CAS Consumer for aggregate."; }
            public String argname()     { return "descriptor xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyCC.xml"; }
            public String deflt()       { return null; }
            public String label()       { return "DriverDescriptorCC"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessDescriptorCCOverrides { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_CC_overrides; }
            public String argname()     { return "list of overrides"; }
            public String description() { return "Process CAS Consumer configuration parameter name/value pair overrides. Parameters must already be defined in the CC descriptor."; }
            public String example()     { return "name1=value1,name2=\"value2a value2b value2c\",name3=value3..."; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessDescriptorCCOverrides"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessDescriptorCM { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_CM; }
            public String description() { return "Process CAS Multiplier for aggregate."; }
            public String argname()     { return "descriptor xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyCM.xml"; }
            public String deflt()       { return null; }
            public String label()       { return "DriverDescriptorCM"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessDescriptorCMOverrides { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_CM_overrides; }
            public String argname()     { return "list of overrides"; }
            public String description() { return "Process CAS Multiplier configuration parameter name/value pair overrides. Parameters must already be defined in the CM descriptor."; }
            public String example()     { return "name1=value1,name2=\"value2a value2b value2c\",name3=value3..."; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessDescriptorCMOverrides"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            



        ProcessDD { 
            public String pname()       { return JobSpecificationProperties.key_process_DD; }
            public String description() { return "Process deployment descriptor (mutually exclusive with CM+AE+CC)."; }
            public String argname()     { return "dd.xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyDD.xml"; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessDD"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessDeploymentsMax { 
            public String pname()       { return JobSpecificationProperties.key_process_deployments_max; }
            public String description() { return "Maximum number of processes dispatched for this job at any time.."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessDeploymentsMax"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        ProcessEnvironment { 
            public String pname()       { return JobSpecificationProperties.key_process_environment; }
            public String argname()     { return "environment-var-list"; }
            public String description() { return "Blank delimeted list of Environment variables."; }
            public String example()     { return "A=B MYENV=foo"; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessEnvironment"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessExecutable { 
            public String pname()       { return JobSpecificationProperties.key_process_executable; }
            public String argname()     { return "program name"; }
            public String description() { return "The full path to a program to be executed."; }
            public String example()     { return "/bin/ls"; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessExecutableArgs { 
            public String pname()       { return JobSpecificationProperties.key_process_executable_args; }
            public String argname()     { return "argument list"; }
            public String description() { return "Blank-delimited list of arguments for " + ProcessExecutable.pname(); }
            public String example()     { return "-a -t -l"; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessGetMetaTimeMax { 
            public String pname()       { return JobSpecificationProperties.key_process_get_meta_time_max; }
            public String description() { return "Maximum elapsed time (in minutes) for processing getMeta."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        ProcessInitializationFailuresCap { 
            public String pname()       { return JobSpecificationProperties.key_process_initialization_failures_cap; }
            public String description() { return "Maximum number of independent job process initialization failures (i.e. System.exit(), kill-15...) before the number of Job Processes is capped at the number in state Running currently.  Default is " + deflt() + "."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String deflt()       { return "99"; }
            public String label()       { return "ProcessInitializationFailuresCap"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessFailuresLimit { 
            public String pname()       { return JobSpecificationProperties.key_process_failures_limit; }
            public String description() { return "Maximum number of independent job process failures (i.e. System.exit(), kill-15...) before job is terminated."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String deflt()       { return "20"; }
            public String label()       { return "ProcessFailuresLimit"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        ProcessJvmArgs { 
            public String pname()       { return JobSpecificationProperties.key_process_jvm_args; }
            public String argname()     { return "jvm arguments"; }
            public String description() { return "Blank-delimeted list of JVM Arguments passed to each process"; }
            public String example()     { return "-Xmx100M -DMYVAR=foo"; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessJvmArgs"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        ProcessMemorySize { 
            public String pname()       { return JobSpecificationProperties.key_process_memory_size; }
            public String argname()     { return "size-in-GB"; }
            public String description() { return "Maximum memory for each process, in GB."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessMemorySize"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessThreadCount { 
            public String pname()       { return JobSpecificationProperties.key_process_thread_count; }
            public String description() { return "Number of pipelines per deployment (i.e. UIMA pipelines per UIMA-AS service copy)."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String deflt()       { return "4"; }
            public String label()       { return "ProcessThreadCount"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ProcessPerItemTimeMax { 
            public String pname()       { return JobSpecificationProperties.key_process_per_item_time_max; }
            public String description() { return "Maximum elapsed time (in minutes) for processing one CAS."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Query       { 
            public String pname()       { return "query"; } 
            public String argname()     { return null; } 
            public String description() { return "Query registered services." ; } 
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },

        Reason { 
            // generated, not public
            public String pname()       { return JobRequestProperties.key_reason; }
            public String argname()     { return "quoted text"; }
            public String description() { return "Reason for the cancel"; }
            public String example()     { return "Back to the drawing board"; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ReservationMemorySize { 
            public String pname()       { return ReservationSpecificationProperties.key_instance_memory_size; }
            public String argname()     { return "size[KB|MB|GB|TB]"; }
            public String description() { return "Size of instance's memory, defaults to GB if units omitted."; }
            public String example()     { return "64GB"; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return true; }
        },            

        ReservationNodeList { 
            // generated, not public
            public String pname()       { return ReservationRequestProperties.key_node_list; }
            public String argname()     { return "string"; }
            public String description() { return "Set of nodes reserved - internall generated."; }
            public String example()     { return "Back to the drawing board"; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        SchedulingClass { 
            public String pname()       { return JobSpecificationProperties.key_scheduling_class; }
            public String argname()     { return "scheduling class name"; }
            public String description() { return "The class to run the job in."; }
            public String example()     { return "A=B MYENV=foo"; }
            public String deflt()       { return null; }
            public String label()       { return "SchedulingClass"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return true; }
        },            

        ServiceLinger { 
            public String pname()       { return "service_linger"; }
            public String argname()     { return "seconds"; }
            public String description() { return "Time in milliseconds to wait after last referring job or service exits before stopping a non-autostarted service."; }
            public String example()     { return "--service_linger 1000"; }
            public String deflt()       { return "300000"; } // 5 minutes
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ServiceDependency { 
            public String pname()       { return JobSpecificationProperties.key_service_dependency; }
            public String argname()     { return "list"; }
            public String description() { return "List of service descriptor strings."; }
            public String example()     { return "UIMA-AS:RandomSleepAE:tcp://bluej672:61616 CUSTOM:myservice";}
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        ServiceRequestEndpoint { 
            // registration option
            public String pname()      { return ServiceRequestProperties.key_service_request_endpoint; }
            public String argname()     { return "string"; }
            public String description() { return "Unique id for this service. Usually inferred for UIMA-AS services."; }
            public String example()     { return "UIMA-AS:queueName:ActiveMqUrl"; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        Signature { 
            // generated, not public
            public String pname()       { return JobSpecificationProperties.key_signature; }
            public String argname()     { return null; }
            public String description() { return null; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        Specification { 
            public String pname()       { return JobSpecificationProperties.key_specification; }
            public String argname()     { return "file"; }
            public String description() { return "Properties file comprising the specification, where the keys are names of parameters. Individual parameters take precedence over those specified in properties file, if any."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        SubmitErrors { 
            // generated, not public
            public String pname()       { return JobSpecificationProperties.key_submit_errors; }
            public String argname()     { return null; }
            public String description() { return null; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        SubmitWarnings { 
            // generated, not public
            public String pname()       { return JobSpecificationProperties.key_submit_warnings; }
            public String argname()     { return null; }
            public String description() { return null; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        Timestamp { 
            public String pname()       { return "timestamp"; }
            public String argname()     { return null; }
            public String description() { return "Enables timestamp on monitor messages."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return "ProcessTimestamp"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        Unregister  { 
            public String pname()       { return "unregister" ; } 
            public String argname()     { return "service-id-or-endpoint" ; } 
            public String description() { return "Unregister a service." ; } 
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },

        Update { 
            public String pname()       { return "update"; }
            public String argname()     { return null; }
            public String description() { return "If specified, update service registry with accompanying parameters."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        User { 
            public String pname()       { return JobSpecificationProperties.key_user; };
            public String argname()     { return "userid"; }
            public String description() { return "Filled in (and overridden) by the CLI, the id of the submitting user."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return null; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            


        WaitForCompletion { 
            public String pname()       { return "wait_for_completion"; }
            public String argname()     { return null; }
            public String description() { return "Do not exit until job is completed."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return name(); }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            

        WorkingDirectory { 
            public String pname()       { return JobSpecificationProperties.key_working_directory; }
            public String argname()     { return "path"; }
            public String description() { return "The working directory set in each process. Default to current directory."; }
            public String example()     { return null; }
            public String deflt()       { return null; }
            public String label()       { return "WorkingDirectory"; }
            public boolean multiargs()  { return false; }
            public boolean required()   { return false; }
        },            
        ;
                  
        public abstract String  pname();            // name of the option, e.g.  --description
        public abstract String  argname();          // type of its argument, or null if none
        public abstract boolean multiargs();        // the option can have >1 arg
        public abstract boolean required();         // this option is required
        public abstract String  description();      // description of what it is
        public abstract String  example();          // example of usage
        public abstract String  deflt();            // defalt, or ""
        public abstract String  label();            // Parameter name for label in web form

        public String makeDesc()
        {
            if ( example() == null ) return description();
            return description() + "\nexample: " + example();
        }
    };

    public enum ClasspathOrderParms
    {    	    	
        UserBeforeDucc   { 
            public String pname()      { return DuccUiConstants.classpath_order_user_before_ducc; } 
            public String description() { return "Start process with user's classpath ahead of DUCC's"; }
        },
        DuccBeforeUser   { 
            public String pname()      { return DuccUiConstants.classpath_order_ducc_before_user; } 
            public String description() { return "Start process with DUCC's classpath ahead of user's"; }
        },
        Unknown {
            public String pname()      { return "unknown"; }
            public String description() { return "Illegal argument"; }
        },
        ;

        public abstract String pname();
        public abstract String description();

   };

}
