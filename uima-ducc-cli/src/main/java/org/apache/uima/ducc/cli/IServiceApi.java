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

import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;

/**
 * These are the constants supported by the 'not-using-a-props-file' form of registration in DuccServiceApi.
 * 
 * There are here so that hopefully we can avoid touching anything in common or transport when updating
 * the registration parameters.
 */

public interface IServiceApi
{
    //
    // decode() mean convert the enum into the string the user uses
    // encode() means take the user's string and turn it into th enum
    // description() is a short description of the option for the commons cli parser
    // argname()     is a name for the argument for the usage() part of cli parser
    //

    public enum ClasspathOrderParms
    {    	    	
        UserBeforeDucc   { 
            public String decode()      { return DuccUiConstants.classpath_order_user_before_ducc; } 
            public String description() { return "Start process with uer's classpath ahead of DUCC's"; }
            public String argname()     { return "none"; }
        },
        DuccBeforeUser   { 
            public String decode()      { return DuccUiConstants.classpath_order_ducc_before_user; } 
            public String description() { return "Start process with DUCC's classpath ahead of user's"; }
            public String argname()     { return "none"; }
        },
        Unknown {
            public String decode()      { return "unknown"; }
            public String description() { return "Illegal argument"; }
            public String argname()     { return "none"; }
        },
        ;

        public abstract String decode();
        public abstract String description();
        public abstract String argname();

        public static ClasspathOrderParms  encode(String value)
        {
            if ( value.equals(UserBeforeDucc.decode()) )  return UserBeforeDucc;
            if ( value.equals(DuccBeforeUser.decode()) )  return DuccBeforeUser;
            return Unknown;
        }
   };

    public enum RegistrationOption
    {
        ClasspathOrder { 
            public String decode()      { return "classpath_order"; }
            public String description() { return "Specify user-supplied classpath before DUCC-supplied classpath, or the reverse."; }
            public String argname()     { return ClasspathOrderParms.UserBeforeDucc.decode() + " or " + ClasspathOrderParms.DuccBeforeUser.decode(); }
        },            

        Description { 
            public String decode()      { return "description"; }
            public String description() { return "Description of the run"; }
            public String argname()     { return "description-string"; }
        },            

        ProcessDD { 
            public String decode()      { return "process_DD"; }
            public String description() { return "Process deployment descriptor."; }
            public String argname()     { return "dd-descriptor.xml"; }
        },            

        ProcessClasspath { 
            public String decode()      { return "process_classpath"; }
            public String description() { return "Classpath for the processes, inherited from environment if not specified"; }
            public String argname()     { return "classpath-string"; }
        },            

        ProcessEnvironment { 
            public String decode()      { return "process_environment"; }
            public String description() { return "Blank delimeted list of Environment variables; for example, ENV1=foo ENV2=bar"; }
            public String argname()     { return "environment-var-list"; }
        },            

        ProcessFailuresLimit { 
            public String decode()      { return "process_failures_limit"; }
            public String description() { return "Maximimum number of failures allowed before the job is automatically cancelled."; }
            public String argname()     { return "environment-var-list"; }
        },            

        ProcessJvmArgs { 
            public String decode()      { return "process_jvm_args"; }
            public String description() { return "Blank-delimeted list of JVM Arguments passed to each process"; }
            public String argname()     { return "argument-list"; }
        },            

        ProcessMemorySize { 
            public String decode()      { return "process_memory_size"; }
            public String description() { return "Maximum memory usage of each process, in GB."; }
            public String argname()     { return "size-in-GB"; }
        },            

        SchedulingClass { 
            public String decode()      { return "scheduling_class"; }
            public String description() { return "The class to run the job in."; }
            public String argname()     { return "scheduling-class"; }
        },            

        ServiceRequestEndpoint { 
            public String decode()      { return ServiceRequestProperties.key_service_request_endpoint; }
            public String description() { return "Unique id for this service. Usually inferred for UIMA-AS services."; }
            public String argname()     { return "string"; }
        },            

        ServicePingClass { 
            public String decode()      { return "service_ping_class"; }
            public String description() { return "Class to ping ervice, must extend AServicePing.java"; }
            public String argname()     { return "classname"; }
        },            

        ServicePingClasspath { 
            public String decode()      { return "service_ping_classpath"; }
            public String description() { return "Classpath containing service_custom_ping class and dependencies."; }
            public String argname()     { return "classpath"; }
        },            

        ServicePingJvmArgs { 
            public String decode()      { return "service_ping_jvm_args"; }
            public String description() { return "-D jvm system property assignments to pass to jvm"; }
            public String argname()     { return "java-system-property-assignments"; }
        },            

        ServicePingTimeout { 
            public String decode()      { return "service_ping_timeout"; }
            public String description() { return "Time in milliseconds to wait for a ping to the service."; }
            public String argname()     { return "time-in-ms"; }
        },            

        ServicePingDoLog { 
            public String decode()      { return "service_ping_do_log"; }
            public String description() { return "If specified, log the pinger, else suppress the log."; }
            public String argname()     { return "none"; }
        },            

        WorkingDirectory { 
            public String decode()      { return "working_directory"; }
            public String description() { return "The working directory set in each process."; }
            public String argname()     { return "directory-name"; }
        },            

        LogDirectory { 
            public String decode()      { return "log_directory"; }
            public String description() { return "The directory where logs are written.  Default: $HOME/ducc/logs"; }
            public String argname()     { return "directory-name"; }
        },            

        Jvm { 
            public String decode()      { return "jvm"; }
            public String description() { return "The jvm to use.  Must be a full path to the 'java' executable.  Default is\n   the jvm that DUCC is using."; }
            public String argname()     { return "path-name-to-java"; }
        },            

        ServiceDependency { 
            public String decode()      { return "service_dependency"; }
            public String description() { return "Comma-delimeted list of service descriptor strings. Example:UIMA-AS:RandomSleepAE:tcp://bluej672:61616"; }
            public String argname()     { return "service-dependency-list"; }
        },            

        ServiceLinger { 
            public String decode()      { return "service_linger"; }
            public String description() { return "Time in seconds to wait after last referring job or service exits before stopping a non-autostarted service."; }
            public String argname()     { return "seconds"; }
        },            

        Unknown {
            public String decode()      { return "unknown"; }
            public String description() { return "Illegal argument"; }
            public String argname()     { return "none"; }
        },
        ;
        
        public abstract String decode();
        public abstract String description();
        public abstract String argname();

        // description
        // process_DD
        // process_memory_size
        // process_classpath
        // process_jvm_args
        // process_environment
        // process_failures_limit
        // scheduling_class
        // working directory
        // log_directory
        // jvm
        // service_dependency

        public static RegistrationOption  encode(String value)
        {
            if ( value.equals(Description.decode()) )          return Description;
            if ( value.equals(ProcessDD.decode()) )            return ProcessDD;
            if ( value.equals(ProcessMemorySize.decode()) )    return ProcessMemorySize;
            if ( value.equals(ProcessClasspath.decode()) )     return ProcessClasspath;
            if ( value.equals(ProcessJvmArgs.decode()) )       return ProcessJvmArgs;
            if ( value.equals(ProcessEnvironment.decode()) )   return ProcessEnvironment;
            if ( value.equals(ProcessFailuresLimit.decode()) ) return ProcessFailuresLimit;
            if ( value.equals(SchedulingClass.decode()) )      return SchedulingClass;
            if ( value.equals(WorkingDirectory.decode()) )     return WorkingDirectory;
            if ( value.equals(LogDirectory.decode()) )         return LogDirectory;
            if ( value.equals(ClasspathOrder.decode()) )       return ClasspathOrder;
            if ( value.equals(Jvm.decode()) )                  return Jvm;
            if ( value.equals(ServiceDependency.decode()) )    return ServiceDependency;
            if ( value.equals(ServiceDependency.decode()) )    return ServiceDependency;
            return Unknown;
        }

    };

    public enum ServiceOptions
    {
        Activate   { 
            public String decode()      { return "activate"; } 
            public String description() { return "If True, sync the active instances with the end result of the modification."; } 
            public String argname()     { return "none"; }
        },

        Autostart   { 
            public String decode()      { return "autostart"; } 
            public String description() { return "If True, start the service when DUCC starts."; } 
            public String argname()     { return "boolean: true or false"; } 
        },

        Instances   { 
            public String decode()      { return "instances"; } 
            public String description() { return "Number of instances to start or stop."; } 
            public String argname()     { return "integer"; } 
        },

        MonitorPort { 
            public String decode()      { return "monitor-port"; } 
            public String description() { return "Activemq JMX port."; } 
            public String argname()     { return "integer"; } 
        },

        Update     { 
            public String decode()      { return "update"; } 
            public String description() { return "Update registry with start or stop."; } 
            public String argname()     { return "none"; } 
        },

        Unknown     { 
            public String decode()      { return "unknown"; } 
            public String description() { return "unknown"; } 
            public String argname()     { return "unknown"; } 
        },
        ;
        
        public abstract String decode();
        public abstract String description();
        public abstract String argname();

        public static ServiceOptions encode(String value)
        {
            if ( value.equals(Activate.decode()) )    return Activate;
            if ( value.equals(Autostart.decode()) )   return Autostart;
            if ( value.equals(Instances.decode()) )   return Instances;
            if ( value.equals(MonitorPort.decode()) ) return MonitorPort;
            if ( value.equals(Update.decode()) )      return Update;

            return Unknown;
        }

    };

}
