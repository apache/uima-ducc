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
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.JobSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;


public class DuccUiConstants {
	
	public static final int ERROR = -1;
	public static final int help_width = 120;
	
	public static final String user = System.getProperty("user.name");
	
	public static final String job_specification_properties = "job-specification.properties";
	public static final String service_specification_properties = "service-specification.properties";
	public static final String managed_reservation_properties = "managed-reservation.properties";
	
	/*
	 * common
	 */
	
	public static final String abrv_help = "h";
	public static final String name_help = "help";
	public static final String desc_help = "Display this message.";
	public static final String labl_help = "Help";
	public static final String exmp_help = "";
	public static final String dval_help = null;

	public static final String name_role_administrator = SpecificationProperties.key_role_administrator;
	public static final String desc_role_administrator = "If CLI invoker is listed in resources/ducc.administrators then allow cancellation on behalf of any user";
	public static final String labl_role_administrator = "Administrator";
	public static final String exmp_role_administrator = "";
	public static final String dval_role_administrator = null;
	
	//public static final String abrv_timestamp = "t";
	public static final String name_timestamp = "timestamp";
	public static final String desc_timestamp = "Timestamp messages.";
	public static final String labl_timestamp = "Timestamp";
	public static final String exmp_timestamp = "";
	public static final String dval_timestamp = null;
	
	//public static final String abrv_debug = "d";
	public static final String name_debug = "debug";
	public static final String desc_debug = "Display extra information.";
	public static final String labl_debug = "Debug";
	public static final String exmp_debug = "";
	public static final String dval_debug = null;

    // Remote debug and console support
	public static final String name_process_debug = JobRequestProperties.key_process_debug;
	public static final String desc_process_debug = "Start remote Job Process in Eclipse debug mode.";
	public static final String labl_process_debug = "ProcessDebug";
	public static final String exmp_process_debug = "";
	public static final String dval_process_debug = null;

	public static final String name_driver_debug = JobRequestProperties.key_driver_debug;
	public static final String desc_driver_debug = "Start remote Job Driver in Eclipse debug mode.";
	public static final String labl_driver_debug = "DriverDebug";
	public static final String exmp_driver_debug = "";
	public static final String dval_driver_debug = null;

	public static final String name_process_attach_console = JobRequestProperties.key_process_attach_console;
	public static final String desc_process_attach_console = "Direct remote Job Process console to the local console.";
	public static final String labl_process_attach_console = "ProcessAttachConsole";
	public static final String exmp_process_attach_console = "";
	public static final String dval_process_attach_console = null;

	public static final String name_driver_attach_console = JobRequestProperties.key_driver_attach_console;
	public static final String desc_driver_attach_console = "Direct remote Job Driver console to the local console.";
	public static final String labl_driver_attach_console = "DriverAttachConsole";
	public static final String exmp_driver_attach_console = "";
	public static final String dval_driver_attach_console = null;
	// End remote debug and console support
	
	/*
	 * cancel
	 */
	
	public static final String name_job_id = JobRequestProperties.key_id;
	public static final String parm_job_id = "number";
	public static final String desc_job_id = "DUCC Job Id.";
	public static final String labl_job_id = "DUCC Job Id";
	public static final String exmp_job_id = "4321";
	public static final String dval_job_id = null;
	
	public static final String name_reservation_id = JobRequestProperties.key_id;
	public static final String parm_reservation_id = "number";
	public static final String desc_reservation_id = "DUCC Reservation Id.";
	public static final String labl_reservation_id = "DUCC Reservation Id";
	public static final String exmp_reservation_id = "1234";
	public static final String dval_reservation_id = null;

	public static final String name_service_id = JobRequestProperties.key_id;
	public static final String parm_service_id = "number";
	public static final String desc_service_id = "DUCC Service Id.";
	public static final String labl_service_id = "DUCC Service Id";
	public static final String exmp_service_id = "4321";
	public static final String dval_service_id = null;
	
	public static final String name_djpid = JobRequestProperties.key_dpid;
	public static final String parm_djpid = "number";
	public static final String desc_djpid = "DUCC Process Id.  If specified only this DUCC process will be canceled.  If not specified, then entire job will be canceled.";
	public static final String labl_djpid = "DUCC Process Id";
	public static final String exmp_djpid = "22";
	public static final String dval_djpid = null;
	
	public static final String name_reason = JobRequestProperties.key_reason;
	public static final String parm_reason = "\"text\"";
	public static final String desc_reason = "Reason.";
	public static final String labl_reason = "Reason";
	public static final String exmp_reason = "\"Back to the drawing board.\"";
	public static final String dval_reason = null;
	
	/* 
	 * submit common
	 */
	
	public static final String name_description = JobSpecificationProperties.key_description;
	public static final String parm_description = "\"text\"";
	public static final String desc_description = "Description of the run.";
	public static final String labl_description = "Description";
	public static final String exmp_description = "\"This is my description.\"";
	public static final String dval_description = null;
	
	public static final String name_notifications = JobSpecificationProperties.key_notifications;
	public static final String parm_notifications = "list";
	public static final String desc_notifications = "Notification sent upon job completion.";
	public static final String labl_notifications = "Notifications";
	public static final String exmp_notifications = "e.g. user1@hostA,user2@hostB...";
	public static final String dval_notifications = null;
	
	public static final String name_specification = JobSpecificationProperties.key_specification;
	public static final String parm_specification = "file";
	public static final String desc_specification = "Properties file comprising the specification, where the keys are names of parameters. Individual parameters take precedence over those specified in properties file, if any.";
	
	/* 
	 * submit Job
	 */
	
	public static final String name_scheduling_class = JobSpecificationProperties.key_scheduling_class;
	public static final String parm_scheduling_class = "level";
	public static final String desc_scheduling_class = "Scheduling class for the run.";
	public static final String labl_scheduling_class = "Scheduling Class";
	public static final String exmp_scheduling_class = "normal";
	public static final String dval_scheduling_class = "normal";
	
	public static final String name_reservation_scheduling_class = JobSpecificationProperties.key_scheduling_class;
	public static final String parm_reservation_scheduling_class = "level";
	public static final String desc_reservation_scheduling_class = "Scheduling class for the run.";
	public static final String labl_reservation_scheduling_class = "Scheduling Class";
	public static final String exmp_reservation_scheduling_class = "reserve";
	public static final String dval_reservation_scheduling_class = "reserve";
	
	public static final String name_scheduling_priority = JobSpecificationProperties.key_scheduling_priority;
	public static final String parm_scheduling_priority = "level";
	public static final String desc_scheduling_priority = "Scheduling priority for the run.";
	public static final String labl_scheduling_priority = "Scheduling Priority";
	public static final String exmp_scheduling_priority = "0";
	public static final String dval_scheduling_priority = "0";
	
	public static final String name_log_directory = JobSpecificationProperties.key_log_directory;
	public static final String parm_log_directory = "path";
	public static final String desc_log_directory = "Log directory (must be writable!).  Default is user's home directory.";
	public static final String labl_log_directory = "Log Directory";
	public static final String exmp_log_directory = System.getProperty("user.home");
	public static final String dval_log_directory = "";
	
	public static final String name_working_directory = JobSpecificationProperties.key_working_directory;
	public static final String parm_working_directory = "path";
	public static final String desc_working_directory = "Working directory.  Default is user's current directory.";
	public static final String labl_working_directory = "Working Directory";
	public static final String exmp_working_directory = System.getProperty("user.dir");
	public static final String dval_working_directory = "";
	
	public static final String name_jvm = JobSpecificationProperties.key_jvm;
	public static final String parm_jvm = "jvm";
	public static final String desc_jvm = "The JVM to employ for both Job Driver and Job Process(es).";
	public static final String labl_jvm = "Jvm";
	public static final String exmp_jvm = "/usr/X11R6/bin/java";
	public static final String dval_jvm = "";
	
	public static final String name_driver_jvm_args = JobSpecificationProperties.key_driver_jvm_args;
	public static final String parm_driver_jvm_args = "args";
	public static final String desc_driver_jvm_args = "Driver JVM args.";
	public static final String labl_driver_jvm_args = "Jvm Args";
	public static final String exmp_driver_jvm_args = "-Xmx100M -Xms50M";
	public static final String dval_driver_jvm_args = "";
	
	public static final String name_driver_classpath = JobSpecificationProperties.key_driver_classpath;
	public static final String parm_driver_classpath = "classpath";
	public static final String desc_driver_classpath = "Driver classpath.  Default is current classpath.";
	public static final String labl_driver_classpath = "Classpath";
	public static final String exmp_driver_classpath = "";
	public static final String dval_driver_classpath = "";
	
	public static final String name_driver_environment = JobSpecificationProperties.key_driver_environment;
	public static final String parm_driver_environment = "environment";
	public static final String desc_driver_environment = "Driver environment.";
	public static final String labl_driver_environment = "Environment";
	public static final String exmp_driver_environment = "\"TERM=xterm DISPLAY=:1.0\"";
	public static final String dval_driver_environment = "";
	
	public static final String name_driver_memory_size = JobSpecificationProperties.key_driver_memory_size;
	public static final String parm_driver_memory_size = "size[KB|MB|GB|TB]";
	public static final String desc_driver_memory_size = "Size of memory for driver, defaults to GB if units omitted.";
	public static final String labl_driver_memory_size = "Memory Size";
	public static final String exmp_driver_memory_size = "16GB";
	public static final String dval_driver_memory_size = "";
	
	public static final String name_driver_descriptor_CR = JobSpecificationProperties.key_driver_descriptor_CR;
	public static final String parm_driver_descriptor_CR = "descriptor.xml";
	public static final String desc_driver_descriptor_CR = "Driver Collection Reader.";
	public static final String labl_driver_descriptor_CR = "CR Descriptor";
	public static final String exmp_driver_descriptor_CR = "/home/"+user+"/MyWorkspace/MyProject/resources/descriptors/com/ibm/ducc/uima/MyCR.xml";
	public static final String dval_driver_descriptor_CR = "";
	
	public static final String name_driver_descriptor_CR_overrides = JobSpecificationProperties.key_driver_descriptor_CR_overrides;
	public static final String parm_driver_descriptor_CR_overrides = "list";
	public static final String desc_driver_descriptor_CR_overrides = "Driver Collection Reader configuration parameter name/value pair overrides. Parameters must already be defined in the CR descriptor.";
	public static final String labl_driver_descriptor_CR_overrides = "CR Overrides.";
	public static final String exmp_driver_descriptor_CR_overrides = "name1=value1,name2=\"value2a value2b value2c\",name3=value3...";
	public static final String dval_driver_descriptor_CR_overrides = "";
	
	public static final String name_driver_exception_handler = JobSpecificationProperties.key_driver_exception_handler;
	public static final String parm_driver_exception_handler = "path.Classname";
	public static final String desc_driver_exception_handler = "Driver Exception handler class.  Must implement "+IJdProcessExceptionHandler.class.getName();
	public static final String labl_driver_exception_handler = "Driver Exception handler.";
	public static final String exmp_driver_exception_handler = "org.apache.uima.ducc.myProject.MyDriverExceptionHandler";
	public static final String dval_driver_exception_handler = "org.apache.uima.ducc.common.jd.plugin.JdProcessExceptionHandler";
	
	public static final String name_process_jvm_args = JobSpecificationProperties.key_process_jvm_args;
	public static final String parm_process_jvm_args = "args";
	public static final String desc_process_jvm_args = "Process JVM args.";
	public static final String labl_process_jvm_args = "Jvm Args";
	public static final String exmp_process_jvm_args = "-Xmx400M -Xms100M";
	public static final String dval_process_jvm_args = "";
	
	public static final String name_process_classpath = JobSpecificationProperties.key_process_classpath;
	public static final String parm_process_classpath = "classpath";
	public static final String desc_process_classpath = "Process classpath.  Default is current classpath.";
	public static final String labl_process_classpath = "Classpath";
	public static final String exmp_process_classpath = "";
	public static final String dval_process_classpath = "";
	
	public static final String name_process_environment = JobSpecificationProperties.key_process_environment;
	public static final String parm_process_environment = "environment";
	public static final String desc_process_environment = "Process environment.";
	public static final String labl_process_environment = "Environment";
	public static final String exmp_process_environment = "\"LANG=en_US.UTF-8\"";
	public static final String dval_process_environment = "";
	
	public static final String name_process_executable = "process_executable";
	public static final String parm_process_executable = "string";
	public static final String desc_process_executable = "Executable program.";
	public static final String labl_process_executable = "Executable";
	public static final String exmp_process_executable = "/bin/sleep";
	public static final String dval_process_executable = "";
	
	public static final String name_process_executable_args = "process_executable_args";
	public static final String parm_process_executable_args = "Argument String";
	public static final String desc_process_executable_args = "The process arguments.";
	public static final String labl_process_executable_args = "Arguments";
	public static final String exmp_process_executable_args = "-i 20 -f out.file";
	public static final String dval_process_executable_args = "";
	
	public static final String name_process_memory_size = JobSpecificationProperties.key_process_memory_size;
	public static final String parm_process_memory_size = "size[KB|MB|GB|TB]";
	public static final String desc_process_memory_size = "Size of memory for process, defaults to GB if units omitted.";
	public static final String labl_process_memory_size = "Memory Size";
	public static final String exmp_process_memory_size = "32GB";
	public static final String dval_process_memory_size = "";
	
	public static final String name_process_DD = JobSpecificationProperties.key_process_DD;
	public static final String parm_process_DD = "dd.xml";
	public static final String desc_process_DD = "Process deployment descriptor (mutually exclusive with CM+AE+CC).";
	public static final String labl_process_DD = "DD";
	public static final String exmp_process_DD = "/home/"+user+"/MyWorkspace/MyProject/resources/descriptors/com/ibm/ducc/uima/MyDD.xml";
	public static final String dval_process_DD = "";
	
	public static final String name_process_descriptor_CM = JobSpecificationProperties.key_process_descriptor_CM;
	public static final String parm_process_descriptor_CM = "descriptor.xml";
	public static final String desc_process_descriptor_CM = "Process CAS Multiplier for aggregate.";
	public static final String labl_process_descriptor_CM = "CM Descriptor";
	public static final String exmp_process_descriptor_CM = "/home/"+user+"/MyWorkspace/MyProject/resources/descriptors/com/ibm/ducc/uima/MyCM.xml";
	public static final String dval_process_descriptor_CM = "";
	
	public static final String name_process_descriptor_CM_overrides = JobSpecificationProperties.key_process_descriptor_CM_overrides;
	public static final String parm_process_descriptor_CM_overrides = "list";
	public static final String desc_process_descriptor_CM_overrides = "Process CAS Multiplier configuration parameter name/value pair overrides. Parameters must already be defined in the CM descriptor.";
	public static final String labl_process_descriptor_CM_overrides = "CM Overrides.";
	public static final String exmp_process_descriptor_CM_overrides = "name1=value1,name2=value2...";
	public static final String dval_process_descriptor_CM_overrides = "";
	
	public static final String name_process_descriptor_AE = JobSpecificationProperties.key_process_descriptor_AE;
	public static final String parm_process_descriptor_AE = "descriptor.xml";
	public static final String desc_process_descriptor_AE = "Process CAS Analysis Engine for aggregate.";
	public static final String labl_process_descriptor_AE = "AE Descriptor";
	public static final String exmp_process_descriptor_AE = "/home/"+user+"/MyWorkspace/MyProject/resources/descriptors/com/ibm/ducc/uima/MyAE.xml";
	public static final String dval_process_descriptor_AE = "";
	
	public static final String name_process_descriptor_AE_overrides = JobSpecificationProperties.key_process_descriptor_AE_overrides;
	public static final String parm_process_descriptor_AE_overrides = "list";
	public static final String desc_process_descriptor_AE_overrides = "Process Analysis Engine configuration parameter name/value pair overrides. Parameters must already be defined in the AE descriptor.";
	public static final String labl_process_descriptor_AE_overrides = "AE Overrides.";
	public static final String exmp_process_descriptor_AE_overrides = "name1=value1,name2=value2...";
	public static final String dval_process_descriptor_AE_overrides = "";
	
	public static final String name_process_descriptor_CC = JobSpecificationProperties.key_process_descriptor_CC;
	public static final String parm_process_descriptor_CC = "descriptor.xml";
	public static final String desc_process_descriptor_CC = "Process CAS Consumer for aggregate.";
	public static final String labl_process_descriptor_CC = "CC Descriptor";
	public static final String exmp_process_descriptor_CC = "/home/"+user+"/MyWorkspace/MyProject/resources/descriptors/com/ibm/ducc/uima/MyCC.xml";
	public static final String dval_process_descriptor_CC = "";
	
	public static final String name_process_descriptor_CC_overrides = JobSpecificationProperties.key_process_descriptor_CC_overrides;
	public static final String parm_process_descriptor_CC_overrides = "list";
	public static final String desc_process_descriptor_CC_overrides = "Process CAS Consumer configuration parameter name/value pair overrides. Parameters must already be defined in the CC descriptor.";
	public static final String labl_process_descriptor_CC_overrides = "CC Overrides.";
	public static final String exmp_process_descriptor_CC_overrides = "name1=value1,name2=value2...";
	public static final String dval_process_descriptor_CC_overrides = "";
	
	public static final String name_process_deployments_max = JobSpecificationProperties.key_process_deployments_max;
	public static final String parm_process_deployments_max = "integer";
	public static final String desc_process_deployments_max = "Maximum number of deployments (i.e. UIMA-AS service copies).";
	public static final String labl_process_deployments_max = "Max Deployments";
	public static final String exmp_process_deployments_max = "5";
	public static final String dval_process_deployments_max = "";
	
	public static final String name_process_deployments_min = JobSpecificationProperties.key_process_deployments_min;
	public static final String parm_process_deployments_min = "integer";
	public static final String desc_process_deployments_min = "Minimum number of deployments (i.e. UIMA-AS service copies).";
	public static final String labl_process_deployments_min = "Min Deployments";
	public static final String exmp_process_deployments_min = "1";
	public static final String dval_process_deployments_min = "1";
	
	public static final String default_process_initialization_failures_cap = "99";
	public static final String name_process_initialization_failures_cap = JobSpecificationProperties.key_process_initialization_failures_cap;
	public static final String parm_process_initialization_failures_cap = "integer";
	public static final String desc_process_initialization_failures_cap = "Maximum number of independent job process initialization failures (i.e. System.exit(), kill-15...) before the number of Job Processes is capped at the number in state Running currently.  Default is "+default_process_initialization_failures_cap+".";
	public static final String labl_process_initialization_failures_cap = "Max Process Initialization Failures";
	public static final String exmp_process_initialization_failures_cap = "10";
	public static final String dval_process_initialization_failures_cap = default_process_initialization_failures_cap;
	
	public static final String name_process_failures_limit = JobSpecificationProperties.key_process_failures_limit;
	public static final String parm_process_failures_limit = "integer";
	public static final String desc_process_failures_limit = "Maximum number of independent job process failures (i.e. System.exit(), kill-15...) before job is terminated.";
	public static final String labl_process_failures_limit = "Max Process Failures";
	public static final String exmp_process_failures_limit = "20";
	public static final String dval_process_failures_limit = "20";
	
	public static final String name_process_thread_count = JobSpecificationProperties.key_process_thread_count;
	public static final String parm_process_thread_count = "integer";
	public static final String desc_process_thread_count = "Number of pipelines per deployment (i.e. UIMA pipelines per UIMA-AS service copy).";
	public static final String labl_process_thread_count = "Threads";
	public static final String exmp_process_thread_count = "4";
	public static final String dval_process_thread_count = "4";
	
	public static final String name_process_get_meta_time_max = JobSpecificationProperties.key_process_get_meta_time_max;
	public static final String parm_process_get_meta_time_max = "integer";
	public static final String desc_process_get_meta_time_max = "Maximum elapsed time (in minutes) for processing getMeta.";
	public static final String labl_process_get_meta_time_max = "Time Max: initalize";
	public static final String exmp_process_get_meta_time_max = "2";
	public static final String dval_process_get_meta_time_max = "2";
	
	public static final String name_process_per_item_time_max = JobSpecificationProperties.key_process_per_item_time_max;
	public static final String parm_process_per_item_time_max = "integer";
	public static final String desc_process_per_item_time_max = "Maximum elapsed time (in minutes) for processing one CAS.";
	public static final String labl_process_per_item_time_max = "Time Max: per CAS";
	public static final String exmp_process_per_item_time_max = "1";
	public static final String dval_process_per_item_time_max = "1";
	
	public static final String name_wait_for_completion = "wait_for_completion";
	public static final String parm_wait_for_completion = null;
	public static final String desc_wait_for_completion = "Do not exit until job is completed.";
	public static final String labl_wait_for_completion = null;
	public static final String exmp_wait_for_completion = null;
	public static final String dval_wait_for_completion = null;
	
	public static final String name_submit_cancel_job_on_interrupt = "cancel_job_on_interrupt";
	public static final String parm_submit_cancel_job_on_interrupt = null;
	public static final String desc_submit_cancel_job_on_interrupt = "Cancel job on interrupt (Ctrl-C); only possible when --"+name_wait_for_completion+" is also specified.";
	public static final String labl_submit_cancel_job_on_interrupt = null;
	public static final String exmp_submit_cancel_job_on_interrupt = null;
	public static final String dval_submit_cancel_job_on_interrupt = null;
	
	public static final String classpath_order_user_before_ducc = "user-before-ducc";
	public static final String classpath_order_ducc_before_user = "ducc-before-user";
	public static final String classpath_order_default = classpath_order_user_before_ducc;
	
	public static final String name_classpath_order = JobSpecificationProperties.key_classpath_order;
	public static final String parm_classpath_order = classpath_order_user_before_ducc+"|"+classpath_order_ducc_before_user;
	public static final String desc_classpath_order = "Classpath order.  Default is configured by administrator, normally "+classpath_order_default+".";
	public static final String labl_classpath_order = null;
	public static final String exmp_classpath_order = null;
	public static final String dval_classpath_order = null;
	
	/* 
	 * monitor Job
	 */
	
	public static final String name_monitor_cancel_job_on_interrupt = name_submit_cancel_job_on_interrupt;
	public static final String parm_monitor_cancel_job_on_interrupt = parm_submit_cancel_job_on_interrupt;
	public static final String desc_monitor_cancel_job_on_interrupt = "Cancel job on interrupt (Ctrl-C).";
	public static final String labl_monitor_cancel_job_on_interrupt = labl_submit_cancel_job_on_interrupt;
	public static final String exmp_monitor_cancel_job_on_interrupt = exmp_submit_cancel_job_on_interrupt;
	public static final String dval_monitor_cancel_job_on_interrupt = dval_submit_cancel_job_on_interrupt;
	
	/* 
	 * submit Managed Reservation
	 */
	
	public static final String name_submit_cancel_managed_reservation_on_interrupt = "cancel_managed_reservation_on_interrupt";
	public static final String parm_submit_cancel_managed_reservation_on_interrupt = null;
	public static final String desc_submit_cancel_managed_reservation_on_interrupt = "Cancel managed_reservation on interrupt (Ctrl-C); only possible when --"+name_wait_for_completion+" is also specified.";
	public static final String labl_submit_cancel_managed_reservation_on_interrupt = null;
	public static final String exmp_submit_cancel_managed_reservation_on_interrupt = null;
	public static final String dval_submit_cancel_managed_reservation_on_interrupt = null;
	
	/* 
	 * monitor Managed Reservation
	 */
	
	public static final String name_monitor_cancel_managed_reservation_on_interrupt = name_submit_cancel_managed_reservation_on_interrupt;
	public static final String parm_monitor_cancel_managed_reservation_on_interrupt = parm_submit_cancel_managed_reservation_on_interrupt;
	public static final String desc_monitor_cancel_managed_reservation_on_interrupt = "Cancel managed_reservation on interrupt (Ctrl-C).";
	public static final String labl_monitor_cancel_managed_reservation_on_interrupt = labl_submit_cancel_managed_reservation_on_interrupt;
	public static final String exmp_monitor_cancel_managed_reservation_on_interrupt = exmp_submit_cancel_managed_reservation_on_interrupt;
	public static final String dval_monitor_cancel_managed_reservation_on_interrupt = dval_submit_cancel_managed_reservation_on_interrupt;
	
	/* 
	 * submit Service
	 */
	
	public static final String name_service = "service";
	public static final String parm_service = null;
	public static final String desc_service = "job is a service and therefore has no driver.";
	public static final String labl_service = null;
	public static final String exmp_service = null;
	public static final String dval_service = null;
	
	public static final String name_service_type_uima = ServiceRequestProperties.key_service_type_uima; // default
	public static final String parm_service_type_uima = null;
	public static final String desc_service_type_uima = "UIMA service type.";
	public static final String labl_service_type_uima = null;
	public static final String exmp_service_type_uima = null;
	public static final String dval_service_type_uima = null;
	
	public static final String name_service_type_custom = ServiceRequestProperties.key_service_type_custom;
	public static final String parm_service_type_custom = null;
	public static final String desc_service_type_custom = "custom service type.";
	public static final String labl_service_type_custom = null;
	public static final String exmp_service_type_custom = null;
	public static final String dval_service_type_custom = null;
	
	public static final String name_service_type_other = ServiceRequestProperties.key_service_type_other;
	public static final String parm_service_type_other = null;
	public static final String desc_service_type_other = "other service type.";
	public static final String labl_service_type_other = null;
	public static final String exmp_service_type_other = null;
	public static final String dval_service_type_other = null;
	
	/* 
	 * submit Reservation
	 */
	
	public static final String name_instance_memory_size = ReservationSpecificationProperties.key_instance_memory_size;
	public static final String parm_instance_memory_size = "size[KB|MB|GB|TB]";
	public static final String desc_instance_memory_size = "Size of instance's memory, defaults to GB if units omitted.";
	public static final String labl_instance_memory_size = "instance Memory Size";
	public static final String exmp_instance_memory_size = "64GB";
	public static final String dval_instance_memory_size = "";
	
	public static final String name_number_of_instances = ReservationSpecificationProperties.key_number_of_instances;
	public static final String parm_number_of_instances = "integer";
	public static final String desc_number_of_instances = "Number of instances to reserve.";
	public static final String labl_number_of_instances = "Number of instances";
	public static final String exmp_number_of_instances = "1";
	public static final String dval_number_of_instances = "1";

	public static final String name_service_dependency = JobSpecificationProperties.key_service_dependency;;
	public static final String parm_service_dependency = "list";
	public static final String desc_service_dependency = "Comma-delimeted list of service descriptor strings.";
	public static final String labl_service_dependency = "Service Descriptors";
	public static final String exmp_service_dependency = "UIMA-AS:RandomSleepAE:tcp://bluej672:61616";
	public static final String dval_service_dependency = "";
}
