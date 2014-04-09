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
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;

/*
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

    static final int help_width = 120;

    enum UiOption
    {
        Activate   { 
            public String pname()       { return "activate"; } 
            public String argname()     { return null; }
            public String description() { return "If present, apply current service updates to the running instances.."; } 
            public String example()     { return null; }
            public String label()       { return name(); }
        },
        
        AllInOne   { 
            public String pname()      { return "all_in_one"; } 
            public String argname()     { return "local|remote"; } 
            public String description() { return "Run driver and pipeline in single process."; } 
            public String example()     { return null; }
            public String label()       { return name(); }
        },
        
        AttachConsole { 
            public String pname()       { return "attach_console"; }
            public String argname()     { return null; }
            public String description() { return "If specified, redirect remote stdout and stderr to the local submitting console."; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },            

        Autostart   { 
            public String pname()      { return "autostart"; } 
            public String argname()     { return "boolean: true or false"; } 
            public String description() { return "If True, start the service when DUCC starts."; } 
            public String example()     { return null; }
            public String label()       { return name(); }
        },

        CancelOnInterrupt { 
            public String pname()       { return SpecificationProperties.key_cancel_on_interrupt; }
            public String argname()     { return null; }
            public String description() { return "Cancel on interrupt (Ctrl-C). Implies "+WaitForCompletion.pname(); }
            public String example()     { return null; }
            public String label()       { return name(); }
        },       
        
        Classpath { 
            public String pname()       { return JobSpecificationProperties.key_classpath; }
            public String description() { return "Classpath for the Job. Default is current classpath."; }
            public String argname()     { return "java classpath"; }
            public String example()     { return null; }
            public String label()       { return "ClassPath"; }
        },    
        
        ClasspathOrder { 
            public String pname()       { return JobSpecificationProperties.key_classpath_order; }
            public String argname()     { return ClasspathOrderParms.UserBeforeDucc.pname() + " or " + ClasspathOrderParms.DuccBeforeUser.pname(); }
            public String description() { return "Specify user-supplied classpath before DUCC-supplied classpath, or the reverse."; }
            public String example()     { return null; }
            public String deflt()       { return ClasspathOrderParms.DuccBeforeUser.pname(); }
            public String label()       { return name(); }
        },            

        Debug { 
            public String pname()       { return "debug"; }
            public String argname()     { return null; }
            public String description() { return "Enable CLI Debugging messages."; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        DjPid { 
            public String pname()       { return JobRequestProperties.key_dpid; }
            public String argname()     { return "number"; }
            public String description() { return "DUCC Process Id.  If specified only this DUCC process will be canceled.  If not specified, then entire job will be canceled.";}
            public String example()     { return "22"; }
            public String label()       { return "Ducc Process Id"; }
        },            

        DriverDebug { 
            public String pname()       { return "driver_debug"; }
            public String argname()     { return "debugger-port-number"; }
            public String description() { return "Append JVM debug flags to the jvm arguments to start the JobDriver in remote debug mode."; }
            public String example()     { return "driver_debug 8001"; }
            public String label()       { return "ProcessDebug"; }
        },            

        Description { 
            public String pname()       { return JobSpecificationProperties.key_description; }
            public String argname()     { return "string"; }
            public String description() { return "Description of the run."; }
            public String example()     { return "My excellent job!"; }
            public String deflt()       { return "none"; }
            public String label()       { return "Description"; }
        },            

        DriverDescriptorCR { 
            public String pname()       { return JobSpecificationProperties.key_driver_descriptor_CR; }
            public String description() { return "Driver (collection reader) descriptor."; }
            public String argname()     { return "descriptor xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyCR.xml"; }
            public String label()       { return "DriverDescriptorCR"; }
            public boolean required()   { return true; }
        },            

        DriverDescriptorCROverrides { 
            public String pname()       { return JobSpecificationProperties.key_driver_descriptor_CR_overrides; }
            public String argname()     { return "list of overrides"; }
            public String description() { return "Driver Collection Reader configuration parameter name/value pair overrides. Parameters must already be defined in the CR descriptor."; }
            public String example()     { return "name1=value1,name2=\"value2a value2b value2c\",name3=value3..."; }
            public String label()       { return "DriverDescriptorCROverrides"; }
        },            

        DriverExceptionHandler { 
            public String pname()       { return JobSpecificationProperties.key_driver_exception_handler; }
            public String description() { return "Driver Exception handler class.  Must implement "+IJdProcessExceptionHandler.class.getName(); }
            public String argname()     { return "classname"; }
            public String example()     { return "org.bob.myProject.MyDriverExceptionHandler"; }
            public String label()       { return "DriverExceptionHandler"; }
        },            

        DriverJvmArgs { 
            public String pname()       { return JobSpecificationProperties.key_driver_jvm_args; }
            public String argname()     { return "jvm arguments"; }
            public String description() { return "Blank-delimited list of JVM Arguments passed to the job driver."; }
            public String example()     { return "-Xmx100M -DMYVAR=foo"; }
            public String label()       { return "DriverJvmArgs"; }
        },    
        
        Environment { 
            public String pname()       { return JobSpecificationProperties.key_environment; }
            public String argname()     { return "env vars"; }
            public String description() { return "Blank-delimited list of environment variables."; }
            public String example()     { return "TERM=xterm DISPLAY=me.org.net:1.0"; }
            public String label()       { return "Environment"; }
        },   
        
        Help { 
            public String pname()       { return "help"; }
            public String argname()     { return null; }
            public String description() { return "Print this help message"; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        Instances   { 
            public String pname()       { return "instances"; } 
            public String argname()     { return "integer"; } 
            public String description() { return "Number of service processes."; } 
            public String example()     { return null; }
            public String label()       { return name(); }
        },
        
        InstanceFailureLimit { 
            public String pname()       { return "instance_failures_limit"; }
            public String description() { return "Number of consecutive instance failures that will cause the service to be stopped."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },   

        InstanceInitFailureLimit { 
            public String pname()       { return "instance_init_failures_limit"; }
            public String description() { return "Number of consecutive instance initialization failures that will cause SM to cease starting the service."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },   

        InstanceFailureWindow { 
            public String pname()       { return "instance_failures_window"; }
            public String argname()     { return "integer"; }
            public String description() { return "Size of the window (in minutes) used to manage service instance runtime failures."; }
            public String example()     { return "30"; }
            public String label()       { return name(); }
        },            

        JobId { 
            public String pname()       { return JobRequestProperties.key_id; }
            public String argname()     { return "string"; }
            public String description() { return "The id of the job"; }
            public String example()     { return null; }
            public String label()       { return null; }
            public boolean required()   { return true; }
        },
        
        ManagedReservationId { 
            public String pname()       { return JobRequestProperties.key_id; }
            public String argname()     { return "string"; }
            public String description() { return "The id of the managed reservation"; }
            public String example()     { return null; }
            public String label()       { return null; }
            public boolean required()   { return true; }
        },      
        
        Jvm { 
            public String pname()       { return JobSpecificationProperties.key_jvm; }
            public String argname()     { return "path-name-to-java"; }
            public String description() { return "The jvm to use.  Must be a full path to the 'java' executable.  Default is\n   the jvm that DUCC is using."; }
            public String example()     { return null; }
            public String label()       { return "Jvm"; }
        },            

        LogDirectory { 
            public String pname()       { return JobSpecificationProperties.key_log_directory; }
            public String argname()     { return "path"; }
            public String description() { return "The directory where logs are written.  Default: $HOME/ducc/logs"; }
            public String example()     { return null; }
            public String label()       { return "LogDirectory"; }
        },            

        Message { 
            public String pname()       { return JobReplyProperties.key_message; }
            public String argname()     { return "string"; }
            public String description() { return "Orchestrator response string - internally generated."; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        Modify    { 
             public String pname()      { return "modify"; } 
             public String argname()     { return "service-id-or-endpoint" ; } 
             public String description() { return "Modify meta properties for a registered service." ; } 
             public String example()     { return null; }
             public String label()       { return name(); }
             public boolean required()   { return true; }
        },

        NumberOfInstances { 
            public String pname()       { return ReservationSpecificationProperties.key_number_of_instances; }
            public String argname()     { return "integer"; }
            public String description() { return "Number of instances to reserve"; }
            public String example()     { return "1"; }
            public String deflt()       { return "1"; }
            public String label()       { return "Number of instances"; }
        },  
        
        Quiet { 
            public String pname()       { return "quiet"; }
            public String argname()     { return null; }
            public String description() { return "Disable CLI Informational messages."; }
            public String example()     { return null; }
            public String label()       { return null; }
        },  
        
        Register    { 
            public String pname()       { return "register"; } 
            public String argname()     { return "specification-file (optional)"; } 
            public String description() { return "Register a service."; } 
            public String example()     { return null; }
            public String label()       { return name(); }
        },

        ServicePingArguments { 
            public String pname()       { return "service_ping_arguments"; }
            public String argname()     { return "string"; }
            public String description() { return "Any (service-dependent) ping arguments, to be passed to the pinger."; }
            public String example()     { return "q_thresh=12,svc_thresh=.01"; }
            public String label()       { return name(); }
        },            

        ServicePingClass { 
            public String pname()       { return "service_ping_class"; }
            public String argname()     { return "classname"; }
            public String description() { return "Class to ping ervice, must extend AServicePing.java"; }
            public String example()     { return "org.bob.Pinger"; }
            public String label()       { return name(); }
        },            

        ServicePingClasspath { 
            public String pname()       { return "service_ping_classpath"; }
            public String argname()     { return "classpath"; }
            public String description() { return "Classpath containing service_custom_ping class and dependencies."; }
            public String example()     { return "Bob.jar"; }
            public String label()       { return name(); }
        },            

        ServicePingJvmArgs { 
            public String pname()       { return "service_ping_jvm_args"; }
            public String argname()     { return "java-system-property-assignments"; }
            public String description() { return "-D jvm system property assignments to pass to jvm"; }
            public String example()     { return "-DxmX=3"; }
            public String label()       { return name(); }
        },            

        ServicePingTimeout { 
            public String pname()       { return "service_ping_timeout"; }
            public String argname()     { return "time-in-ms"; }
            public String description() { return "Time in milliseconds to wait for a ping to the service."; }
            public String example()     { return "1000"; }
            public String label()       { return name(); }
        },            

        ServicePingDoLog { 
            public String pname()       { return "service_ping_dolog"; }
            public String argname()     { return "boolean"; }
            public String description() { return "If specified, log the pinger, else suppress the log."; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },            

        ServiceTypeCustom { 
            public String pname()       { return ServiceRequestProperties.key_service_type_custom; }
            public String argname()     { return null; }
            public String description() { return "Service type - internally generated"; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        ServiceTypeOther { 
            public String pname()       { return ServiceRequestProperties.key_service_type_other; }
            public String argname()     { return null; }
            public String description() { return "Service type - internally generated"; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        ServiceTypeUima { 
            public String pname()       { return ServiceRequestProperties.key_service_type_uima; }
            public String argname()     { return null; }
            public String description() { return "Service type - internally generated"; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        Start       { 
            public String pname()       { return "start"; } 
            public String description() { return "Start a registered service." ; } 
            public String argname()     { return "service-id-or-endpoint" ; } 
            public String example()     { return null; }
            public String label()       { return null; }
        },

        Stop        { 
            public String pname()      { return "stop"; } 
            public String description() { return "Stop a registered service." ; } 
            public String argname()     { return "service-id-or-endpoint" ; } 
            public String example()     { return null; }
            public String label()       { return null; }
        },

        SubmitPid { 
            // generated
            public String pname()       { return JobRequestProperties.key_submitter_pid_at_host; }
            public String argname()     { return null; }
            public String description() { return null; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        ProcessDebug { 
            public String pname()       { return "process_debug"; }
            public String argname()     { return "debugger-port-number"; }
            public String description() { return "Listening port number the remote process is to connect to."; }
            public String example()     { return "8000"; }
            public String label()       { return "ProcessDebug"; }
        },            

        ProcessDebugHost {              // inserted by service registration so we can remember where to call home
            public String pname()       { return "process_debug_host"; }
            public String argname()     { return "debugger-host-name"; }
            public String description() { return "Nodename where java debugger is listening for connection.  Only valid for services."; }
            public String example()     { return "gallifrey"; }
            public String label()       { return "ProcessDebugHost"; }
        },            

        ProcessDescriptorAE { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_AE; }
            public String description() { return "Process Analysis Enginefor aggregate."; }
            public String argname()     { return "descriptor xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyAE.xml"; }
            public String label()       { return "DriverDescriptorAE"; }
        },            

        ProcessDescriptorAEOverrides { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_AE_overrides; }
            public String argname()     { return "list of overrides"; }
            public String description() { return "Process Analysis Engine configuration parameter name/value pair overrides. Parameters must already be defined in the AE descriptor."; }
            public String example()     { return "name1=value1,name2=\"value2a value2b value2c\",name3=value3..."; }
            public String label()       { return "ProcessDescriptorAEOverrides"; }
        },            

        ProcessDescriptorCC { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_CC; }
            public String description() { return "Process CAS Consumer for aggregate."; }
            public String argname()     { return "descriptor xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyCC.xml"; }
            public String label()       { return "DriverDescriptorCC"; }
        },            

        ProcessDescriptorCCOverrides { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_CC_overrides; }
            public String argname()     { return "list of overrides"; }
            public String description() { return "Process CAS Consumer configuration parameter name/value pair overrides. Parameters must already be defined in the CC descriptor."; }
            public String example()     { return "name1=value1,name2=\"value2a value2b value2c\",name3=value3..."; }
            public String label()       { return "ProcessDescriptorCCOverrides"; }
        },            

        ProcessDescriptorCM { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_CM; }
            public String description() { return "Process CAS Multiplier for aggregate."; }
            public String argname()     { return "descriptor xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyCM.xml"; }
            public String label()       { return "DriverDescriptorCM"; }
        },            

        ProcessDescriptorCMOverrides { 
            public String pname()       { return JobSpecificationProperties.key_process_descriptor_CM_overrides; }
            public String argname()     { return "list of overrides"; }
            public String description() { return "Process CAS Multiplier configuration parameter name/value pair overrides. Parameters must already be defined in the CM descriptor."; }
            public String example()     { return "name1=value1,name2=\"value2a value2b value2c\",name3=value3..."; }
            public String label()       { return "ProcessDescriptorCMOverrides"; }
        },            

        ProcessDD { 
            public String pname()       { return JobSpecificationProperties.key_process_DD; }
            public String description() { return "Process deployment descriptor (mutually exclusive with CM+AE+CC)."; }
            public String argname()     { return "dd.xml"; }
            public String example()     { return "/home/" + System.getProperty("user.name") + "/descriptors/MyDD.xml"; }
            public String label()       { return "ProcessDD"; }
        },            

        ProcessDeploymentsMax { 
            public String pname()       { return JobSpecificationProperties.key_process_deployments_max; }
            public String description() { return "Maximum number of processes dispatched for this job at any time.."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String label()       { return "ProcessDeploymentsMax"; }
        },            

        ProcessExecutable { 
            public String pname()       { return JobSpecificationProperties.key_process_executable; }
            public String argname()     { return "program name"; }
            public String description() { return "The full path to a program to be executed."; }
            public String example()     { return "/bin/ls"; }
            public String label()       { return name(); }
        },            

        ProcessExecutableArgs { 
            public String pname()       { return JobSpecificationProperties.key_process_executable_args; }
            public String argname()     { return "argument list"; }
            public String description() { return "Blank-delimited list of arguments for " + ProcessExecutable.pname(); }
            public String example()     { return "-a -t -l"; }
            public String label()       { return name(); }
        },            

        ProcessGetMetaTimeMax {
            //public String pname()       { return JobSpecificationProperties.key_process_get_meta_time_max; }
            public String pname()       { return "process_get_meta_time_max"; }
            public String description() { return "Maximum elapsed time (in minutes) for processing getMeta."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },            
		
        ProcessInitializationTimeMax { 
            public String pname()       { return JobSpecificationProperties.key_process_initialization_time_max; }
            public String description() { return DuccUiConstants.desc_process_initialization_time_max; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },     
		
        ProcessInitializationFailuresCap { 
            public String pname()       { return JobSpecificationProperties.key_process_initialization_failures_cap; }
            public String description() { return "Number of unexpected job process initialization failures (i.e. System.exit(), kill-15...) before the number of Job Processes is capped at the number in state Running currently.  Default is " + deflt() + "."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String deflt()       { return "99"; }
            public String label()       { return "ProcessInitializationFailuresCap"; }
        },            

        ProcessFailuresLimit { 
            public String pname()       { return JobSpecificationProperties.key_process_failures_limit; }
            public String description() { return "Number of unexpected job process failures (i.e. System.exit(), kill-15...) that will cause the job to be terminated."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String deflt()       { return "20"; }
            public String label()       { return "ProcessFailuresLimit"; }
        },            

        ProcessJvmArgs { 
            public String pname()       { return JobSpecificationProperties.key_process_jvm_args; }
            public String argname()     { return "jvm arguments"; }
            public String description() { return "Blank-delimited list of JVM Arguments passed to each process"; }
            public String example()     { return "-Xmx100M -DMYVAR=foo"; }
            public String label()       { return "ProcessJvmArgs"; }
        },            

        ProcessMemorySize { 
            public String pname()       { return JobSpecificationProperties.key_process_memory_size; }
            public String argname()     { return "size-in-GB"; }
            public String description() { return "Maximum memory for each process, in GB."; }
            public String example()     { return "30"; }
            public String label()       { return "ProcessMemorySize"; }
        },            

        ProcessThreadCount { 
            public String pname()       { return JobSpecificationProperties.key_process_thread_count; }
            public String description() { return "Number of pipelines per deployment (i.e. UIMA pipelines per UIMA-AS service copy)."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String deflt()       { return "4"; }
            public String label()       { return "ProcessThreadCount"; }
        },            

        ProcessPerItemTimeMax { 
            public String pname()       { return JobSpecificationProperties.key_process_per_item_time_max; }
            public String description() { return "Maximum elapsed time (in minutes) for processing one CAS."; }
            public String argname()     { return "integer"; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },            

        Query       { 
            public String pname()       { return "query"; } 
            public String argname()     { return "service-id-or-endpoint (optional)" ; } 
            public String description() { return "Query a registered service, or all." ; } 
            public String example()     { return null; }
            public String label()       { return name(); }
        },

        Reason { 
            // generated, not public
            public String pname()       { return JobRequestProperties.key_reason; }
            public String argname()     { return "quoted text"; }
            public String description() { return "Reason for the cancel"; }
            public String example()     { return "Back to the drawing board"; }
            public String label()       { return name(); }
        },            

        ReservationMemorySize { 
            public String pname()       { return ReservationSpecificationProperties.key_instance_memory_size; }
            public String argname()     { return "size-in-GB"; }
            public String description() { return "Size of instance's memory, in GB."; }
            public String example()     { return "64"; }
            public String label()       { return name(); }
            public boolean required()   { return true; }
        },            

        ReservationNodeList { 
            // generated, not public
            public String pname()       { return ReservationRequestProperties.key_node_list; }
            public String argname()     { return "string"; }
            public String description() { return "Set of nodes reserved - internall generated."; }
            public String example()     { return "Back to the drawing board"; }
            public String label()       { return null; }
        },            

        RoleAdministrator { 
            public String pname()       { return JobSpecificationProperties.key_role_administrator; }
            public String argname()     { return null; }
            public String description() { return "Act in the capacity of DUCC administrator."; }
            public String example()     { return null; }
            public String label()       { return null; }
        },     
        
        SchedulingClass { 
            public String pname()       { return JobSpecificationProperties.key_scheduling_class; }
            public String argname()     { return "scheduling class name"; }
            public String description() { return "The class to run the job in."; }
            public String example()     { return "normal"; }
            public String label()       { return "SchedulingClass"; }
        },            

        ServiceLinger { 
            public String pname()       { return "service_linger"; }
            public String argname()     { return "milliseconds"; }
            public String description() { return "Time in milliseconds to wait after last referring job or service exits before stopping a non-autostarted service."; }
            public String example()     { return null; }
            public String deflt()       { return "300000"; } // 5 minutes
            public String label()       { return name(); }
        },            

        ServiceDependency { 
            public String pname()       { return JobSpecificationProperties.key_service_dependency; }
            public String argname()     { return "list"; }
            public String description() { return "List of service descriptor strings."; }
            public String example()     { return "UIMA-AS:RandomSleepAE:tcp://node1:61616 CUSTOM:myservice";}
            public String label()       { return name(); }
        },            

        ServiceRequestEndpoint { 
            // registration option
            public String pname()      { return ServiceRequestProperties.key_service_request_endpoint; }
            public String argname()     { return "string"; }
            public String description() { return "Unique id for this service. Usually inferred for UIMA-AS services."; }
            public String example()     { return "UIMA-AS:queueName:ActiveMqUrl"; }
            public String label()       { return name(); }
        },            

        Signature { 
            // generated, not public
            public String pname()       { return JobSpecificationProperties.key_signature; }
            public String argname()     { return null; }
            public String description() { return null; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        Specification { 
            public String pname()       { return JobSpecificationProperties.key_specification; }
            public String sname()       { return "f"; }
            public String argname()     { return "file"; }
            public String description() { return "Properties file comprising the specification, where the keys are names of parameters. Individual parameters take precedence over those specified in properties file, if any."; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },            

        SubmitErrors { 
            // generated, not public
            public String pname()       { return JobSpecificationProperties.key_submit_errors; }
            public String argname()     { return null; }
            public String description() { return null; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        SubmitWarnings { 
            // generated, not public
            public String pname()       { return JobSpecificationProperties.key_submit_warnings; }
            public String argname()     { return null; }
            public String description() { return null; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        SuppressConsoleLog { 
            public String pname()       { return "suppress_console_log"; }
            public String argname()     { return null; }
            public String description() { return "Do not copy stdout to a log file."; }
            public String example()     { return null; }
            public String label()       { return name(); }
        }, 
        
        Timestamp { 
            public String pname()       { return "timestamp"; }
            public String argname()     { return null; }
            public String description() { return "Enables timestamp on monitor messages."; }
            public String example()     { return null; }
            public String label()       { return "ProcessTimestamp"; }
        },            

        Unregister  { 
            public String pname()       { return "unregister" ; } 
            public String argname()     { return "service-id-or-endpoint" ; } 
            public String description() { return "Unregister a service." ; } 
            public String example()     { return null; }
            public String label()       { return null; }
        },

        Update { 
            public String pname()       { return "update"; }
            public String argname()     { return null; }
            public String description() { return "If specified, update service registry with accompanying parameters."; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        User { 
            public String pname()       { return JobSpecificationProperties.key_user; };
            public String argname()     { return "userid"; }
            public String description() { return "Filled in (and overridden) by the CLI, the id of the submitting user."; }
            public String example()     { return null; }
            public String label()       { return null; }
        },            

        WaitForCompletion { 
            public String pname()       { return "wait_for_completion"; }
            public String argname()     { return null; }
            public String description() { return "Do not exit until job is completed."; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },            

        WorkingDirectory { 
            public String pname()       { return JobSpecificationProperties.key_working_directory; }
            public String argname()     { return "path"; }
            public String description() { return "The working directory set in each process. Default to current directory."; }
            public String example()     { return null; }
            public String deflt()       { return "."; }
            public String label()       { return "WorkingDirectory"; }
        },            
        ;
          
        // Each option must implement the first few methods,
        public abstract String  pname();             // name of the option, e.g.  --description
        public abstract String  argname();           // type of its argument, or null if none
        public abstract String  description();       // description of what it is
        public abstract String  example();           // example of usage
        public boolean multiargs() { return false; } // the option can have >1 arg
        public boolean required()  { return false; } // this option is required
        public String  deflt()     { return null; }  // default, or ""
        public String  label()     { return null; }  // Parameter name for label in web form
        public String  sname()     { return null; }  // short name of option

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
