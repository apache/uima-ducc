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
package org.apache.uima.ducc.cli.aio;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.uima.ducc.cli.CliBase;
import org.apache.uima.ducc.cli.DuccJobSubmit;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Level;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Toggle;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;

public class AllInOneLauncher extends CliBase {
    
	private static String cid = AllInOneLauncher.class.getSimpleName();
	
	private static String or_port = "ducc.orchestrator.http.port";
    private static String or_host = "ducc.orchestrator.node";
	
	private static String remote = "remote";
	private static String local = "local";
	
	private String allInOneType = null;
	
	private String jvm = null;
	private String working_directory = null;
	
	private String driver_jvm_args = null;
	private String process_jvm_args = null;
	private String jvm_args = null;
	
	private String driver_classpath = null;
	private String process_classpath = null;
	private String classpath = null;
	
	private String driver_environment = null;
	private String process_environment = null;
	private String environment = null;
	
	private String driver_descriptor_CR = null;
	private String driver_descriptor_CR_overrides = null;
	
	private String process_descriptor_CM = null;
	private String process_descriptor_CM_overrides = null;
	
	private String process_descriptor_AE = null;
	private String process_descriptor_AE_overrides = null;
	
	private String process_descriptor_CC = null;
	private String process_descriptor_CC_overrides = null;
	
	private String process_DD = null;
	
	private boolean classpath_user_first = true;
	
	private IMessageHandler mh = new MessageHandler();
	
	private JobRequestProperties jobRequestProperties = new JobRequestProperties(); 
	
	private String[] args = new String[0];
	private UiOption[] opts = DuccJobSubmit.opts;
	
	public AllInOneLauncher(String[] args) throws Exception {
		this.args = args;
		init(this.getClass().getName(), opts, args, jobRequestProperties, or_host, or_port, "or", consoleCb, null);
	}
	
	private void examine_debug() {
		String mid = "examine_debug";
		debug = jobRequestProperties.containsKey(UiOption.Debug.pname());
		if(debug) {
			mh.setLevel(Level.FrameworkInfo, Toggle.On);
			mh.setLevel(Level.FrameworkDebug, Toggle.On);
			mh.setLevel(Level.FrameworkError, Toggle.On);
			mh.setLevel(Level.FrameworkWarn, Toggle.On);
			String message = "true";
			mh.frameworkDebug(cid, mid, message);
		}
		else {
			String message = "false";
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_timestamp() {
		String mid = "examine_timestamp";
		boolean timestamp = jobRequestProperties.containsKey(UiOption.Timestamp.pname());
		if(timestamp) {
			mh.setTimestamping(Toggle.On);
			String message = "true";
			mh.frameworkDebug(cid, mid, message);
		}
		else {
			String message = "false";
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_allInOne() throws MissingArgumentException, IllegalArgumentException {
		String mid = "examine_allInOne";
		allInOneType = jobRequestProperties.getProperty(UiOption.AllInOne.pname());
        if(allInOneType == null) {
        	throw new MissingArgumentException(UiOption.AllInOne.pname());
        }
        if(allInOneType.equalsIgnoreCase(local)) {
        	String message = allInOneType;
        	mh.frameworkDebug(cid, mid, message);
        }
        else if(allInOneType.equalsIgnoreCase(remote)) {
        	String message = allInOneType;
        	mh.frameworkDebug(cid, mid, message);
        	//TODO
        	throw new IllegalArgumentException(UiOption.AllInOne.pname()+": "+allInOneType+" not supported");
        }
        else {
        	throw new IllegalArgumentException(UiOption.AllInOne.pname()+": "+allInOneType);
        }
	}
	
	private void examine_jvm() {
		String mid = "examine_jvm";
		String key = UiOption.Jvm.pname();
		if(jobRequestProperties.containsKey(key)) {
			jvm = jobRequestProperties.getProperty(key);
			String message = jvm;
			mh.frameworkDebug(cid, mid, message);
		}
		else {
			jvm = System.getProperty("java.home")+File.separator+"bin"+File.separator+"java";
			String message = jvm;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_working_directory() {
		String mid = "examine_working_directory";
		String key = UiOption.WorkingDirectory.pname();
		if(jobRequestProperties.containsKey(key)) {
			working_directory = jobRequestProperties.getProperty(key);
			String message = working_directory;
			mh.frameworkDebug(cid, mid, message);
		}
		else {
			working_directory = System.getProperty("user.dir");
			String message = working_directory;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_driver_jvm_args() {
		String mid = "examine_driver_jvm_args";
		String key = UiOption.DriverJvmArgs.pname();
		if(jobRequestProperties.containsKey(key)) {
			driver_jvm_args = jobRequestProperties.getProperty(key);
			String message = driver_jvm_args;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_jvm_args() {
		String mid = "examine_process_jvm_args";
		String key = UiOption.ProcessJvmArgs.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_jvm_args = jobRequestProperties.getProperty(key);
			String message = process_jvm_args;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_jvm_args() {
		String mid = "examine_jvm_args";
		String key = UiOption.JvmArgs.pname();
		if(jobRequestProperties.containsKey(key)) {
			jvm_args = jobRequestProperties.getProperty(key);
			String message = jvm_args;
			mh.frameworkDebug(cid, mid, message);
		}
		if(jobRequestProperties.containsKey(key)) {
			throw new IllegalArgumentException(UiOption.JvmArgs.pname()+" disallowed");
		}
	}
	
	private void examine_driver_classpath() {
		String mid = "examine_driver_classpath";
		String key = UiOption.DriverClasspath.pname();
		if(jobRequestProperties.containsKey(key)) {
			driver_classpath = jobRequestProperties.getProperty(key);
			String message = driver_classpath;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_classpath() {
		String mid = "examine_process_classpath";
		String key = UiOption.ProcessClasspath.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_classpath = jobRequestProperties.getProperty(key);
			String message = process_classpath;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_classpath() {
		String mid = "examine_classpath";
		String key = UiOption.Classpath.pname();
		if(jobRequestProperties.containsKey(key)) {
			classpath = jobRequestProperties.getProperty(key);
			String message = classpath;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_classpath_order() {
		String mid = "examine_classpath_order";
		String key = UiOption.ClasspathOrder.pname();
		if(jobRequestProperties.containsKey(key)) {
			String value = jobRequestProperties.getProperty(key);
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
		}
	}
	
	private void examine_driver_environment() {
		String mid = "examine_driver_environment";
		String key = UiOption.DriverEnvironment.pname();
		if(jobRequestProperties.containsKey(key)) {
			driver_environment = jobRequestProperties.getProperty(key);
			String message = driver_environment;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_environment() {
		String mid = "examine_process_environment";
		String key = UiOption.ProcessEnvironment.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_environment = jobRequestProperties.getProperty(key);
			String message = process_environment;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void reconcile_jvm_args() {
		String mid = "reconcile_jvm_args";
		if(process_jvm_args != null) {
			jvm_args = process_jvm_args;
			String message = "using "+UiOption.ProcessJvmArgs.pname()+": "+jvm_args;
			mh.frameworkInfo(cid, mid, message);
		}
		if(driver_jvm_args != null) {
			String message = "ignoring "+UiOption.DriverJvmArgs.pname()+": "+jvm_args;
			mh.frameworkWarn(cid, mid, message);
		}
		/*
		if(jvm_args != null) {
			if(process_jvm_args != null) {
				throw new IllegalArgumentException(UiOption.ProcessJvmArgs.pname());
			}
			else if(driver_jvm_args != null) {
				throw new IllegalArgumentException(UiOption.DriverJvmArgs.pname());
			}
		} 
		else if(process_jvm_args != null) {
			jvm_args = process_jvm_args;
			String message = "using process jvm_args: "+jvm_args;
			mh.frameworkInfo(cid, mid, message);
		} 
		else if(driver_jvm_args != null) {
			jvm_args = driver_jvm_args;
			String message = "using driver jvm_args: "+jvm_args;
			mh.frameworkInfo(cid, mid, message);
		}
		else {
			// no default
		}
		*/
	}
	
	private void reconcile_classpath() {
		String mid = "reconcile_classpath";
		if(classpath != null) {
			if(process_classpath != null) {
				throw new IllegalArgumentException(UiOption.ProcessClasspath.pname());
			}
			else if(driver_classpath != null) {
				throw new IllegalArgumentException(UiOption.DriverClasspath.pname());
			}
			String message = "using classpath";
			mh.frameworkDebug(cid, mid, message);
		}
		else if(process_classpath != null) {
			classpath = process_classpath;
			String message = "using process classpath";
			mh.frameworkDebug(cid, mid, message);
		} 
		else if(driver_classpath != null) {
			classpath = driver_classpath;
			String message = "using driver classpath";
			mh.frameworkDebug(cid, mid, message);
		}
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
	
	private void reconcile_environment() {
		String mid = "reconcile_environment";
		if(process_environment != null) {
			environment = process_environment;
			String message = "using process environment: "+environment;
			mh.frameworkInfo(cid, mid, message);
		} 
		else if(driver_environment != null) {
			environment = driver_environment;
			String message = "using driver environment: "+environment;
			mh.frameworkInfo(cid, mid, message);
		}
	}
	
	private void examine_driver_descriptor_CR() {
		String mid = "examine_driver_descriptor_CR";
		String key = UiOption.DriverDescriptorCR.pname();
		if(jobRequestProperties.containsKey(key)) {
			driver_descriptor_CR = jobRequestProperties.getProperty(key);
			String message = key+"="+driver_descriptor_CR;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_driver_descriptor_CR_overrides() {
		String mid = "examine_driver_descriptor_CR_overrides";
		String key = UiOption.DriverDescriptorCROverrides.pname();
		if(jobRequestProperties.containsKey(key)) {
			driver_descriptor_CR_overrides = jobRequestProperties.getProperty(key);
			String message = key+"="+driver_descriptor_CR_overrides;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_descriptor_CM() {
		String mid = "examine_process_descriptor_CM";
		String key = UiOption.ProcessDescriptorCM.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_descriptor_CM = jobRequestProperties.getProperty(key);
			String message = key+"="+process_descriptor_CM;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_descriptor_CM_overrides() {
		String mid = "examine_process_descriptor_CMOverrides";
		String key = UiOption.ProcessDescriptorCMOverrides.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_descriptor_CM_overrides = jobRequestProperties.getProperty(key);
			String message = key+"="+process_descriptor_CM_overrides;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_descriptor_AE() {
		String mid = "examine_process_descriptor_AE";
		String key = UiOption.ProcessDescriptorAE.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_descriptor_AE = jobRequestProperties.getProperty(key);
			String message = key+"="+process_descriptor_AE;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_descriptor_AE_overrides() {
		String mid = "examine_process_descriptor_AE_overrides";
		String key = UiOption.ProcessDescriptorAEOverrides.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_descriptor_AE_overrides = jobRequestProperties.getProperty(key);
			String message = key+"="+process_descriptor_AE_overrides;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_descriptor_CC() {
		String mid = "examine_process_descriptor_CC";
		String key = UiOption.ProcessDescriptorCC.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_descriptor_CC = jobRequestProperties.getProperty(key);
			String message = key+"="+process_descriptor_CC;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_descriptor_CC_overrides() {
		String mid = "examine_process_descriptor_CC_overrides";
		String key = UiOption.ProcessDescriptorCCOverrides.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_descriptor_CC_overrides = jobRequestProperties.getProperty(key);
			String message = key+"="+process_descriptor_CC_overrides;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
	private void examine_process_DD() {
		String mid = "examine_process_DD";
		String key = UiOption.ProcessDD.pname();
		if(jobRequestProperties.containsKey(key)) {
			process_DD = jobRequestProperties.getProperty(key);
			String message = key+"="+process_DD;
			mh.frameworkDebug(cid, mid, message);
		}
	}
	
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
		
		// debug & timestamp
		examine_debug();
		examine_timestamp();
		
		// all_in_one
		examine_allInOne();
		
		// jvm
		examine_jvm();
		
		// working_directory
		examine_working_directory();
		
		// jvm_args
		examine_driver_jvm_args();
		examine_process_jvm_args();
		examine_jvm_args();
		reconcile_jvm_args();
		
		// classpath
		examine_driver_classpath();
		examine_process_classpath();
		examine_classpath();
		examine_classpath_order();
		reconcile_classpath();
		
		// environment
		examine_driver_environment();
		examine_process_environment();
		reconcile_environment();
		
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
		
		if(jvm_args != null) {
			String[] jvmargs = jvm_args.split(" ");
			for(String jvmarg : jvmargs) {
				message = "jvmarg: "+jvmarg;
				mh.frameworkDebug(cid, mid, message);
				commandArray.add(jvmarg);
			}
		}

		commandArray.add("org.apache.uima.ducc.cli.aio.AllInOne");
		
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
	
	private void launch_remote() {
		String mid = "launch_remote";
		mh.frameworkTrace(cid, mid, "enter");
		String message = "type "+allInOneType+" not supported";
		mh.error(cid, mid, message);
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	private void launch() throws IOException {
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
	
	public void go() throws MissingArgumentException, IllegalArgumentException, IOException {
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
