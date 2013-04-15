/*
as * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.uima.ducc.cli.aio;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.uima.ducc.cli.CliBase;
import org.apache.uima.ducc.cli.DuccJobSubmit;
import org.apache.uima.ducc.cli.DuccManagedReservationSubmit;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Level;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Toggle;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;

public class AllInOneLauncher extends CliBase {
    
	private static String cid = AllInOneLauncher.class.getSimpleName();
	
	private static String or_port = "ducc.orchestrator.http.port";
    private static String or_host = "ducc.orchestrator.node";
	
	private static String remote = "remote";
	private static String local = "local";
	
	private static String fixed = "fixed";
	
	private static String enter = "enter";
	private static String exit = "exit";
	
	private String allInOneType = null;
	
	private String attach_console = null;
	
	private String jvm = null;
	private String log_directory = null;
	private String working_directory = null;
	
	private String classpath = null;
	private String environment = null;
	
	private String process_jvm_args = null;
	
	private String driver_descriptor_CR = null;
	private String driver_descriptor_CR_overrides = null;
	
	private String process_descriptor_CM = null;
	private String process_descriptor_CM_overrides = null;
	
	private String process_descriptor_AE = null;
	private String process_descriptor_AE_overrides = null;
	
	private String process_descriptor_CC = null;
	private String process_descriptor_CC_overrides = null;
	
	private String process_DD = null;
	
	private String process_memory_size = null;
	private String description = null;
	private String scheduling_class = fixed;
	
	private String specification = null;
	
	private String signature = null;
	private String user = null;
	
	private boolean wait_for_completion = false;
	private boolean cancel_on_interrupt = false;
	
	private boolean classpath_user_first = true;
	
	private IMessageHandler mh = new MessageHandler();
	
	private JobRequestProperties jobRequestProperties = new JobRequestProperties(); 
	
	private String[] args = new String[0];
	private UiOption[] opts = DuccJobSubmit.opts;
	
	private HashMap<String,String> optionsMap = new HashMap<String,String>();
	
	public AllInOneLauncher(String[] args) throws Exception {
		this.args = args;
		init(this.getClass().getName(), opts, args, jobRequestProperties, or_host, or_port, "or", consoleCb, null);
		String key = DuccPropertiesResolver.ducc_submit_all_in_one_class;
		String value = DuccPropertiesResolver.getInstance().getFileProperty(key);
		if(value != null) {
			scheduling_class = value;
		}
	}
	
	private boolean isLocal() {
		return allInOneType.equalsIgnoreCase(local);
	}
	
	private void ignored() {
		String mid = "ignored";
		mh.frameworkTrace(cid, mid, enter);
		Enumeration<Object> keys = jobRequestProperties.keys();
		while(keys.hasMoreElements()) {
			Object key = keys.nextElement();
			boolean examined = optionsMap.containsKey(key);
			if(!examined) {
				String message = "ignoring "+key;
				mh.warn(cid, mid, message);
			}
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void used(String opt) {
		String mid = "used";
		mh.frameworkTrace(cid, mid, enter);
		optionsMap.put(opt,opt);
		String message = opt;
		mh.frameworkDebug(cid, mid, message);
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void enableDebugFlags() {
		String mid = "enableDebugFlags";
		mh.frameworkTrace(cid, mid, enter);
		mh.setLevel(Level.FrameworkInfo, Toggle.On);
		mh.setLevel(Level.FrameworkDebug, Toggle.On);
		mh.setLevel(Level.FrameworkError, Toggle.On);
		mh.setLevel(Level.FrameworkWarn, Toggle.On);
		mh.frameworkTrace(cid, mid, exit);
	}
	
	// debug
	
	private void examine_debug() {
		String mid = "examine_debug";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.Debug.pname();
		debug = jobRequestProperties.containsKey(pname);
		if(debug) {
			enableDebugFlags();
			String message = "true";
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_debug() {
		String mid = "examine_process_debug";
		mh.frameworkTrace(cid, mid, enter);
		if(jobRequestProperties.containsKey(UiOption.Debug.pname())) {
			return;
		}
		String pname = UiOption.ProcessDebug.pname();
		debug = jobRequestProperties.containsKey(pname);
		if(debug) {
			enableDebugFlags();
			String message = "true";
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_driver_debug() {
		String mid = "examine_driver_debug";
		mh.frameworkTrace(cid, mid, enter);
		mh.frameworkTrace(cid, mid, exit);
	}

	// timestamp
	
	private void examine_timestamp() {
		String mid = "examine_timestamp";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.Timestamp.pname();
		boolean timestamp = jobRequestProperties.containsKey(pname);
		if(timestamp) {
			mh.setTimestamping(Toggle.On);
			String message = "true";
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}

	// all-in-one
	
	private void examine_allInOne() throws MissingArgumentException, IllegalArgumentException {
		String mid = "examine_allInOne";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.AllInOne.pname();
		allInOneType = jobRequestProperties.getProperty(pname);
		if(allInOneType == null) {
        	throw new MissingArgumentException(pname);
        }
		if(allInOneType.equalsIgnoreCase(local)) {
        	String message = allInOneType;
        	mh.frameworkDebug(cid, mid, message);
        	used(pname);
        }
		else if(allInOneType.equalsIgnoreCase(remote)) {
			String message = allInOneType;
	        mh.frameworkDebug(cid, mid, message);
	        used(pname);
	    }
	    else {
	        throw new IllegalArgumentException(pname+": "+allInOneType);
	    }
		mh.frameworkTrace(cid, mid, exit);
	}

	// attach-console
	
	private void examine_process_attach_console() {
		String mid = "examine_process_attach_console";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessAttachConsole.pname();
		if(jobRequestProperties.containsKey(pname)) {
			attach_console = jobRequestProperties.getProperty(pname);
			String message = attach_console;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_driver_attach_console() {
		String mid = "examine_driver_attach_console";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	// jvm
	
	private void examine_jvm() {
		String mid = "examine_jvm";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.Jvm.pname();
		if(jobRequestProperties.containsKey(pname)) {
			jvm = jobRequestProperties.getProperty(pname);
			String message = jvm;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		else {
			jvm = System.getProperty("java.home")+File.separator+"bin"+File.separator+"java";
			String message = jvm;
			mh.frameworkDebug(cid, mid, message);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_log_directory() {
		String mid = "examine_log_directory";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.LogDirectory.pname();
		if(jobRequestProperties.containsKey(pname)) {
			log_directory = jobRequestProperties.getProperty(pname);
			String message = log_directory;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_working_directory() {
		String mid = "examine_working_directory";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.WorkingDirectory.pname();
		if(jobRequestProperties.containsKey(pname)) {
			working_directory = jobRequestProperties.getProperty(pname);
			String message = working_directory;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		else {
			working_directory = System.getProperty("user.dir");
			String message = working_directory;
			mh.frameworkDebug(cid, mid, message);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_jvm_args() {
		String mid = "examine_process_jvm_args";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessJvmArgs.pname();
		if(jobRequestProperties.containsKey(pname)) {
			process_jvm_args = jobRequestProperties.getProperty(pname);
			String message = process_jvm_args;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_driver_jvm_args() {
		String mid = "examine_driver_jvm_args";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_classpath() {
		String mid = "examine_classpath";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.Classpath.pname();
		if(jobRequestProperties.containsKey(pname)) {
			classpath = jobRequestProperties.getProperty(pname);
			String message = classpath;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_classpath() {
		String mid = "examine_process_classpath";
		mh.frameworkTrace(cid, mid, enter);
		if(jobRequestProperties.containsKey(UiOption.Classpath.pname())) {
			return;
		}
		String pname = UiOption.ProcessClasspath.pname();
		if(jobRequestProperties.containsKey(pname)) {
			classpath = jobRequestProperties.getProperty(pname);
			String message = classpath;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_driver_classpath() {
		String mid = "examine_driver_classpath";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_classpath_order() {
		String mid = "examine_classpath_order";
		String pname = UiOption.ClasspathOrder.pname();
		if(jobRequestProperties.containsKey(pname)) {
			String value = jobRequestProperties.getProperty(pname);
			if(value != null) {
				if(value.equalsIgnoreCase(ClasspathOrderParms.UserBeforeDucc.name())) {
					classpath_user_first = true;
				}
				else if(value.equalsIgnoreCase(ClasspathOrderParms.DuccBeforeUser.name())) {
					classpath_user_first = false;
				}
				else {
					throw new IllegalArgumentException(UiOption.ClasspathOrder.pname()+": "+value);
				}
			}
			String message = value;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
	}
	
	private void reconcile_classpath() {
		String mid = "reconcile_classpath";
		if(classpath != null) {
			if(classpath_user_first) {
				classpath = System.getProperty("java.class.path")+File.pathSeparatorChar+classpath;
				String message = "user first";
				mh.frameworkInfo(cid, mid, message);
			}
			else {
				classpath = classpath+File.pathSeparatorChar+System.getProperty("java.class.path");
				String message = "user last";
				mh.frameworkInfo(cid, mid, message);
			}
		}
		else {
			classpath = System.getProperty("java.class.path");
			String message = "user only";
			mh.frameworkInfo(cid, mid, message);
		}
	}
	
	private void examine_environment() {
		String mid = "examine_environment";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.Environment.pname();
		if(jobRequestProperties.containsKey(pname)) {
			environment = jobRequestProperties.getProperty(pname);
			String message = environment;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_environment() {
		String mid = "examine_process_environment";
		mh.frameworkTrace(cid, mid, enter);
		if(jobRequestProperties.containsKey(UiOption.Environment.pname())) {
			return;
		}
		String pname = UiOption.ProcessEnvironment.pname();
		if(jobRequestProperties.containsKey(pname)) {
			environment = jobRequestProperties.getProperty(pname);
			String message = environment;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_driver_environment() {
		String mid = "examine_driver_environment";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_description() {
		String mid = "examine_description";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.Description.pname();
		if(jobRequestProperties.containsKey(pname)) {
			description = jobRequestProperties.getProperty(pname);
			String message = pname+"="+description;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_scheduling_class() {
		String mid = "examine_scheduling_class";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.SchedulingClass.pname();
		if(isLocal()) {
			String message = pname+"="+scheduling_class;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		else if(jobRequestProperties.containsKey(pname)) {
			String user_scheduling_class = jobRequestProperties.getProperty(pname);
			String message = pname+"="+scheduling_class;
			if(user_scheduling_class == null) {
			}
			else if(!user_scheduling_class.trim().equals(scheduling_class)) {
				message = pname+"="+scheduling_class+" "+"replaces"+" "+user_scheduling_class;
			}
			mh.frameworkInfo(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}

	private void examine_process_deployments_max() {
		String mid = "examine_process_deployments_max";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}

	private void examine_process_initialization_failures_cap() {
		String mid = "examine_process_initialization_failures_cap";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_failures_limit() {
		String mid = "examine_process_failures_limit";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_thread_count() {
		String mid = "examine_process_thread_count";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_per_item_time_max() {
		String mid = "examine_process_per_itm_time_max";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_get_meta_time_max() {
		String mid = "examine_process_get_meta_time_max";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_service_dependency() {
		String mid = "examine_service_dependency";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_signature() {
		String mid = "examine_signature";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.Signature.pname();
		if(isLocal()) {
			used(pname);
		}
		if(jobRequestProperties.containsKey(pname)) {
			signature = jobRequestProperties.getProperty(pname);
			String message = pname+"="+signature;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_user() {
		String mid = "examine_user";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.User.pname();
		if(isLocal()) {
			used(pname);
		}
		if(jobRequestProperties.containsKey(pname)) {
			user = jobRequestProperties.getProperty(pname);
			String message = pname+"="+user;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_driver_descriptor_CR() {
		String mid = "examine_driver_descriptor_CR";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.DriverDescriptorCR.pname();
		if(jobRequestProperties.containsKey(pname)) {
			driver_descriptor_CR = jobRequestProperties.getProperty(pname);
			String message = pname+"="+driver_descriptor_CR;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_driver_descriptor_CR_overrides() {
		String mid = "examine_driver_descriptor_CR_overrides";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.DriverDescriptorCROverrides.pname();
		if(jobRequestProperties.containsKey(pname)) {
			driver_descriptor_CR_overrides = jobRequestProperties.getProperty(pname);
			String message = pname+"="+driver_descriptor_CR_overrides;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_driver_exception_handler() {
		String mid = "examine_driver_exception_handler";
		mh.frameworkTrace(cid, mid, enter);
		// ignored
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_memory_size() {
		String mid = "examine_process_memory_size";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessMemorySize.pname();
		if(jobRequestProperties.containsKey(pname)) {
			if(isLocal()) {
				//ignored
			}
			else {
				process_memory_size = jobRequestProperties.getProperty(pname);
				String message = pname+"="+process_memory_size;
				mh.frameworkDebug(cid, mid, message);
				used(pname);
			}
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_DD() {
		String mid = "examine_process_DD";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessDD.pname();
		if(jobRequestProperties.containsKey(pname)) {
			process_DD = jobRequestProperties.getProperty(pname);
			String message = pname+"="+process_DD;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_descriptor_CM() {
		String mid = "examine_process_descriptor_CM";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessDescriptorCM.pname();
		if(jobRequestProperties.containsKey(pname)) {
			process_descriptor_CM = jobRequestProperties.getProperty(pname);
			String message = pname+"="+process_descriptor_CM;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_descriptor_CM_overrides() {
		String mid = "examine_process_descriptor_CMOverrides";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessDescriptorCMOverrides.pname();
		if(jobRequestProperties.containsKey(pname)) {
			process_descriptor_CM_overrides = jobRequestProperties.getProperty(pname);
			String message = pname+"="+process_descriptor_CM_overrides;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_descriptor_AE() {
		String mid = "examine_process_descriptor_AE";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessDescriptorAE.pname();
		if(jobRequestProperties.containsKey(pname)) {
			process_descriptor_AE = jobRequestProperties.getProperty(pname);
			String message = pname+"="+process_descriptor_AE;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_descriptor_AE_overrides() {
		String mid = "examine_process_descriptor_AE_overrides";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessDescriptorAEOverrides.pname();
		if(jobRequestProperties.containsKey(pname)) {
			process_descriptor_AE_overrides = jobRequestProperties.getProperty(pname);
			String message = pname+"="+process_descriptor_AE_overrides;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_descriptor_CC() {
		String mid = "examine_process_descriptor_CC";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessDescriptorCC.pname();
		if(jobRequestProperties.containsKey(pname)) {
			process_descriptor_CC = jobRequestProperties.getProperty(pname);
			String message = pname+"="+process_descriptor_CC;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_process_descriptor_CC_overrides() {
		String mid = "examine_process_descriptor_CC_overrides";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.ProcessDescriptorCCOverrides.pname();
		if(jobRequestProperties.containsKey(pname)) {
			process_descriptor_CC_overrides = jobRequestProperties.getProperty(pname);
			String message = pname+"="+process_descriptor_CC_overrides;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_specification() {
		String mid = "examine_specification";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.Specification.pname();
		if(jobRequestProperties.containsKey(pname)) {
			specification = jobRequestProperties.getProperty(pname);
			String message = pname+"="+specification;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}

	private void examine_wait_for_completion() {
		String mid = "examine_wait_for_completion";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.WaitForCompletion.pname();
		if(jobRequestProperties.containsKey(pname)) {
			wait_for_completion = true;
			String message = pname+"="+wait_for_completion;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_cancel_job_on_interrupt() {
		String mid = "examine_cancel_job_on_interrupt";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.CancelJobOnInterrupt.pname();
		if(jobRequestProperties.containsKey(pname)) {
			cancel_on_interrupt = true;
			wait_for_completion = true;
			String message = pname+"="+cancel_on_interrupt;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_cancel_on_interrupt() {
		String mid = "examine_cancel_on_interrupt";
		mh.frameworkTrace(cid, mid, enter);
		String pname = UiOption.CancelOnInterrupt.pname();
		if(jobRequestProperties.containsKey(pname)) {
			cancel_on_interrupt = true;
			wait_for_completion = true;
			String message = pname+"="+cancel_on_interrupt;
			mh.frameworkDebug(cid, mid, message);
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	private void examine_submitter_pid_at_host() {
		String mid = "examine_submitter_pid_at_host";
		mh.frameworkTrace(cid, mid, enter);
		String pname = "submitter_pid_at_host";
		if(jobRequestProperties.containsKey(pname)) {
			used(pname);
		}
		mh.frameworkTrace(cid, mid, exit);
	}
	
	/////
	
	private void reconcile_descriptors() throws MissingArgumentException {
		if(driver_descriptor_CR == null) {
			throw new MissingArgumentException(UiOption.DriverDescriptorCR.pname());
		}
		if(process_DD != null) {
			if(process_descriptor_CM != null) {
				throw new IllegalArgumentException(UiOption.ProcessDescriptorCM.pname());
			}
			if(process_descriptor_AE != null) {
				throw new IllegalArgumentException(UiOption.ProcessDescriptorAE.pname());
			}
			if(process_descriptor_CC != null) {
				throw new IllegalArgumentException(UiOption.ProcessDescriptorCC.pname());
			}
		}
		else {
			if(process_descriptor_CM == null) {
				if(process_descriptor_CM_overrides != null) {
					throw new IllegalArgumentException(UiOption.ProcessDescriptorCMOverrides.pname());
				}
			}
			if(process_descriptor_AE == null) {
				if(process_descriptor_AE_overrides != null) {
					throw new IllegalArgumentException(UiOption.ProcessDescriptorAEOverrides.pname());
				}
			}
			if(process_descriptor_CC == null) {
				if(process_descriptor_CC_overrides != null) {
					throw new IllegalArgumentException(UiOption.ProcessDescriptorCCOverrides.pname());
				}
			}
			if(process_descriptor_CM == null) {
				if(process_descriptor_AE == null) {
					if(process_descriptor_CC == null) {
						throw new MissingArgumentException(UiOption.ProcessDescriptorAE.pname());
					}
				}
			}
		}
	}
	
	private void examine() throws MissingArgumentException, IllegalArgumentException {
		String mid = "examine";
		mh.frameworkTrace(cid, mid, "enter");
		
		// debug
		examine_debug();
		examine_process_debug();
		examine_driver_debug();
		
		// console
		examine_process_attach_console();
		examine_driver_attach_console();
		
		// timestamp
		examine_timestamp();
		
		// all_in_one
		examine_allInOne();
		
		// jvm
		examine_jvm();
		
		// log_directory
		examine_log_directory();
		
		// working_directory
		examine_working_directory();
		
		// jvm_args
		examine_process_jvm_args();
		examine_driver_jvm_args();
		
		// classpath
		examine_classpath();
		examine_process_classpath();
		examine_driver_classpath();
		examine_classpath_order();
		reconcile_classpath();
		
		// environment
		examine_environment();
		examine_driver_environment();
		examine_process_environment();
		
		// jd
		examine_driver_exception_handler();
		
		// uima
		examine_driver_descriptor_CR();
		examine_driver_descriptor_CR_overrides();
		examine_process_descriptor_CM();
		examine_process_descriptor_CM_overrides();
		examine_process_descriptor_AE();
		examine_process_descriptor_AE_overrides();
		examine_process_descriptor_CC();
		examine_process_descriptor_CC_overrides();
		examine_process_DD();
		reconcile_descriptors();
		
		// memory
		examine_process_memory_size();
		
		// various ignored
		examine_process_deployments_max();
		examine_process_initialization_failures_cap();
		examine_process_failures_limit();
		examine_process_thread_count();
		examine_process_per_item_time_max();
		examine_process_get_meta_time_max();
		examine_service_dependency();
		
		// description
		examine_description();
		
		// scheduling_class
		examine_scheduling_class();
		
		// wait_for_completion & cancel
		examine_wait_for_completion();
		examine_cancel_on_interrupt();
		examine_cancel_job_on_interrupt();
		
		// specification - handled by super()
		examine_specification();
		
		// signature - handled by super()
		examine_signature();
		
		// user - handled by super()
		examine_user();
		
		// submitter_pid_at_host
		examine_submitter_pid_at_host();
		
		// ignored
		ignored();
		
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	private void launch_local() throws IOException {
		String mid = "launch_local";
		mh.frameworkTrace(cid, mid, "enter");
		String message = "local";
		mh.frameworkDebug(cid, mid, message);
		
		ArrayList<String> commandArray = new ArrayList<String>();
		commandArray.add(jvm);
		
		commandArray.add("-classpath");
		commandArray.add(classpath);
		
		String[] cp = classpath.split(":");
		for(String c : cp) {
			message = "classpath: "+c;
			mh.frameworkDebug(cid, mid, message);
		}
		
		if(process_jvm_args != null) {
			String[] jvmargs = process_jvm_args.split(" ");
			for(String jvmarg : jvmargs) {
				message = "jvmarg: "+jvmarg;
				mh.frameworkDebug(cid, mid, message);
				commandArray.add(jvmarg);
			}
		}

		//commandArray.add("org.apache.uima.ducc.cli.aio.AllInOne");
		commandArray.add(AllInOne.class.getCanonicalName());
		
		for(String arg : args) {
			commandArray.add(arg);
		}
		
		String[] command = commandArray.toArray(new String[0]);
		
		ProcessBuilder pb = new ProcessBuilder( command );
		if(working_directory != null) {
			message = "working directory: "+working_directory;
			mh.frameworkDebug(cid, mid, message);
			File wd = new File(working_directory);
			pb.directory(wd);
		}
		
		if(environment != null) {
			Map<String,String> env = pb.environment();
			env.clear();
			String[]envVars = environment.split(" ");
			for(String envVar : envVars) {
				String[] nvp = envVar.trim().split("=");
				if(nvp.length != 2) {
					message = "invalid environment variable specified: "+envVar;
					mh.error(cid, mid, message);
					throw new IllegalArgumentException("invalid environment specified");
				}
				String name = nvp[0];
				String value = nvp[1];
				message = "environment: "+name+"="+value;
				mh.info(cid, mid, message);
				env.put(name, value);
			}
		}
		
		Process process = pb.start();
		
		String line;
		
        InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader ibr = new BufferedReader(isr);
        while ((line = ibr.readLine()) != null) {
            System.out.println(line);
        }
        
        InputStream es = process.getErrorStream();
        InputStreamReader esr = new InputStreamReader(es);
        BufferedReader ebr = new BufferedReader(esr);
        while ((line = ebr.readLine()) != null) {
            System.err.println(line);
        }
        
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	private void addArg(ArrayList<String> cmdLine, String arg) {
		String mid = "launch_remote";
		mh.frameworkTrace(cid, mid, "enter");
		cmdLine.add(arg);
		mh.frameworkDebug(cid, mid, arg);
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	private String getProcessExecutable() {
		String mid = "getProcessExecutable";
		mh.frameworkTrace(cid, mid, "enter");
		StringBuffer sb = new StringBuffer();
		sb.append(System.getProperty("java.home")+File.separator+"bin"+File.separator+"java");
		mh.frameworkTrace(cid, mid, "exit");
		return sb.toString();
	}
	
	private String getProcessExecutableArgs() {
		String mid = "getProcessExecutableArgs";
		mh.frameworkTrace(cid, mid, "enter");
		StringBuffer sb = new StringBuffer();
		if(process_jvm_args != null) {
			sb.append(process_jvm_args);
			sb.append(" ");
		}
		sb.append("-classpath");
		sb.append(" ");
		sb.append(classpath);
		sb.append(" ");
		sb.append(this.getClass().getCanonicalName());
		sb.append(" ");
		boolean skipNext = false;
		for(String arg : args) {
			if(skipNext) {
				skipNext = false;
			}
			else if(arg.trim().equalsIgnoreCase("--"+UiOption.AllInOne.pname())) {
				skipNext = true;
				sb.append(arg);
				sb.append(" ");
				sb.append(local);
				sb.append(" ");
			}
			else if(arg.trim().equalsIgnoreCase("--"+UiOption.DriverClasspath.pname())) {
				skipNext = true;
			}
			else if(arg.trim().equalsIgnoreCase("--"+UiOption.ProcessClasspath.pname())) {
				skipNext = true;
			}
			else if(arg.trim().equalsIgnoreCase("--"+UiOption.Classpath.pname())) {
				skipNext = true;
			}
			else if(arg.trim().equalsIgnoreCase("--"+UiOption.DriverEnvironment.pname())) {
				skipNext = true;
			}
			else if(arg.trim().equalsIgnoreCase("--"+UiOption.ProcessEnvironment.pname())) {
				skipNext = true;
			}
			else if(arg.trim().equalsIgnoreCase("--"+UiOption.Environment.pname())) {
				skipNext = true;
			}
			else {
				sb.append(arg);
				sb.append(" ");
			}
		}
		mh.frameworkTrace(cid, mid, "exit");
		return sb.toString();
	}
	
	private void launch_remote() throws Exception {
		String mid = "launch_remote";
		mh.frameworkTrace(cid, mid, "enter");
		ArrayList<String> cmdLine = new ArrayList<String>();
		addArg(cmdLine, "--"+UiOption.ProcessExecutable.pname());
		addArg(cmdLine, getProcessExecutable());
		addArg(cmdLine, "--"+UiOption.ProcessExecutableArgs.pname());
		addArg(cmdLine, getProcessExecutableArgs());
		addArg(cmdLine, "--"+UiOption.SchedulingClass.pname());
		addArg(cmdLine, scheduling_class);
		if(attach_console != null) {
			addArg(cmdLine, "--"+UiOption.ProcessAttachConsole.pname());
			addArg(cmdLine, attach_console);
		}
		if(environment != null) {
			addArg(cmdLine, "--"+UiOption.Environment.pname());
			addArg(cmdLine, environment);
		}
		if(process_memory_size != null) {
			addArg(cmdLine, "--"+UiOption.ProcessMemorySize.pname());
			addArg(cmdLine, process_memory_size);
		}
		if(log_directory != null) {
			addArg(cmdLine, "--"+UiOption.LogDirectory.pname());
			addArg(cmdLine, log_directory);
		}
		if(working_directory != null) {
			addArg(cmdLine, "--"+UiOption.WorkingDirectory.pname());
			addArg(cmdLine, working_directory);
		}
		if(description != null) {
			addArg(cmdLine, "--"+UiOption.Description.pname());
			addArg(cmdLine, description);
		}
		if(wait_for_completion) {
			addArg(cmdLine, "--"+UiOption.WaitForCompletion.pname());
			addArg(cmdLine, "true");
		}
		if(cancel_on_interrupt) {
			addArg(cmdLine, "--"+UiOption.CancelOnInterrupt.pname());
			addArg(cmdLine, "true");
		}
		String[] argList = cmdLine.toArray(new String[0]);
		DuccManagedReservationSubmit ds = new DuccManagedReservationSubmit(argList);
		boolean rc = ds.execute();
		
		String dt = "Managed Reservation";
		if ( rc ) {
            // Fetch the Ducc ID
        	System.out.println(dt+" "+ds.getDuccId()+" submitted.");
        	int code = 0;
        	if(ds.waitForCompletion()) {
        		code = ds.getReturnCode();
        	}
            if ( code == 1000 ) {
                System.out.println(dt + ": no return code.");
            } else {
                System.out.println(dt+" return code: "+code);
            }
        	System.exit(0);
        } else {
            System.out.println("Could not submit "+dt);
            System.exit(1);
        }
		
		mh.frameworkDebug(cid, mid, "rc="+rc);
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	private void launch() throws Exception {
		String mid = "launch";
		mh.frameworkTrace(cid, mid, "enter");
		if(allInOneType.equalsIgnoreCase(local)) {
			launch_local();
        }
        else if(allInOneType.equalsIgnoreCase(remote)) {
        	launch_remote();
        }
        else {
        	String message = "type "+allInOneType+" not supported";
    		mh.error(cid, mid, message);
        }
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	public void go() throws Exception {
		String mid = "go";
		mh.frameworkTrace(cid, mid, "enter");
		examine();
		launch();
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	public static void main(String[] args) throws Exception {
		AllInOneLauncher allInOneLauncher = new AllInOneLauncher(args);
		allInOneLauncher.go();
	}
	
	@Override
	protected boolean execute() throws Exception {
		return false;
	}
}
