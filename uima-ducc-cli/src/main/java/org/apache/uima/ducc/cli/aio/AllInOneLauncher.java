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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.uima.ducc.cli.CliBase;
import org.apache.uima.ducc.cli.DuccJobSubmit;
import org.apache.uima.ducc.cli.DuccManagedReservationSubmit;
import org.apache.uima.ducc.cli.IDuccCallback;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Level;
import org.apache.uima.ducc.cli.aio.IMessageHandler.Toggle;
import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
import org.apache.uima.ducc.common.utils.QuotedOptions;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;

public class AllInOneLauncher extends CliBase {
    
    private static String cid = AllInOneLauncher.class.getSimpleName();
    
    private static String remote = "remote";
    private static String local = "local";
    
    private static String enter = "enter";
    private static String exit = "exit";
    
    private String allInOneType = null;
    
    private String jvm = null;
    private String log_directory = null;
    private String working_directory = null;
    
    private String classpath = null;
    private String environment = null;
    
    private String process_jvm_args = null;
    private String debug_jvm_args = null;
    
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
    
    private String scheduling_class = null;
    
    private String specification = null;
    
    private String signature = null;
    private String user = null;
    
    private boolean wait_for_completion = false;
    private boolean cancel_on_interrupt = false;
    
    private boolean classpath_user_first = true;
    
    private IMessageHandler mh = new MessageHandler();
    
    private JobRequestProperties jobRequestProperties = new JobRequestProperties(); 
    
    private UiOption[] opts = DuccJobSubmit.opts;
    
    private HashMap<String,String> optionsMap = new HashMap<String,String>();

    public AllInOneLauncher(String[] args) throws Exception {
        init (this.getClass().getName(), opts, args, jobRequestProperties, null);
    }
    
    public AllInOneLauncher(String[] args, IDuccCallback consoleCb) throws Exception {
        mh = new MessageHandler(consoleCb);
        init (this.getClass().getName(), opts, args, jobRequestProperties, consoleCb);
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
        String pname = UiOption.ProcessDebug.pname();
        if (jobRequestProperties.containsKey(pname)) {
            int port = Integer.parseInt(jobRequestProperties.getProperty(pname));
            debug_jvm_args = "-Xdebug -Xrunjdwp:transport=dt_socket,address=" + host_address + ":" + port;         
            mh.frameworkDebug(cid, mid, debug_jvm_args);
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
        String pname = UiOption.AttachConsole.pname();
        if(jobRequestProperties.containsKey(pname)) {
            String message = "attach_console";
            mh.frameworkDebug(cid, mid, message);
            used(pname);
        }
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
            if (debug_jvm_args != null) {
                process_jvm_args += " "+debug_jvm_args;
            }
            String message = process_jvm_args;
            mh.frameworkDebug(cid, mid, message);
            used(pname);
        } else {
            process_jvm_args = debug_jvm_args;
        }
        mh.frameworkTrace(cid, mid, exit);
    }
    
    private void examine_classpath() {
        String mid = "examine_classpath";
        mh.frameworkTrace(cid, mid, enter);
        String pname = UiOption.Classpath.pname();
        classpath = fixupClasspath(pname);
        if (classpath == null) {
            throw new IllegalArgumentException("Specified classpath contains only DUCC jars");
        }
        used(pname);
        String message = classpath;
        mh.frameworkDebug(cid, mid, message);

        // Note: defaulted or validated in init method
        pname = UiOption.ClasspathOrder.pname();
        String value = jobRequestProperties.getProperty(pname);
        used(pname);
        classpath_user_first = value.equals(ClasspathOrderParms.UserBeforeDucc.pname());
        message = value;
        mh.frameworkDebug(cid, mid, message);

        // Don't need all the DUCC jars as user's classpath must have all the UIMA jars it needs.
        // For simplicity add only the jar that has the AllInOne class --- it will pull in other 
        // jars that have dependencies such as the flow controller.
        String duccClasspath = ducc_home + "/lib/uima-ducc-cli.jar";
        if (classpath_user_first) {
            classpath = classpath + File.pathSeparatorChar + duccClasspath;
        } else {
            classpath = duccClasspath + File.pathSeparatorChar + classpath;
        }
        mh.frameworkTrace(cid, mid, exit);
    }
    
    private void examine_environment() {
        String mid = "examine_environment";
        mh.frameworkTrace(cid, mid, enter);
        String pname = UiOption.Environment.pname();
        environment = jobRequestProperties.getProperty(pname);
        if (environment != null) {
            String message = environment;
            mh.frameworkDebug(cid, mid, message);
            used(pname);
        }
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
    
    private void examine_scheduling_class() throws Exception {
        String mid = "examine_scheduling_class";
        mh.frameworkTrace(cid, mid, enter);
        String pname = UiOption.SchedulingClass.pname();
        // If omitted let DUCC choose the default for an AP
        // If a preemptable one change to a fixed one if possible
        if (jobRequestProperties.containsKey(pname)) {
            DuccSchedulerClasses duccSchedulerClasses = DuccSchedulerClasses.getInstance();
            scheduling_class = jobRequestProperties.getProperty(pname);
            String message = pname + "=" + scheduling_class + " [original]";
            if (isLocal()) {
                message = pname + "=" + scheduling_class + " not considered";
                mh.debug(cid, mid, message);
            } else if (duccSchedulerClasses.isPreemptable(scheduling_class)) {
                String specific_scheduling_class = duccSchedulerClasses.getDebugClassSpecificName(scheduling_class);
                if (specific_scheduling_class != null) {
                    scheduling_class = specific_scheduling_class;
                    jobRequestProperties.put(pname, scheduling_class);
                    message = pname + "=" + scheduling_class + " [replacement, specific]";
                    mh.info(cid, mid, message);
                }
            }
            used(pname);
        }
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
    
    private void examine() throws Exception {
        String mid = "examine";
        mh.frameworkTrace(cid, mid, "enter");
        
        // debug
        examine_debug();
        examine_process_debug();
        examine_driver_debug();
        
        // console
        examine_process_attach_console();
        
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
        
        // classpath
        examine_classpath();
        
        // environment
        examine_environment();
        
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
        // DuccJobSubmit does not check for an invalid set
        // Perhaps should be done in CliBase validation
        // reconcile_descriptors();
        
        // memory
        examine_process_memory_size();
        
        // description
        examine_description();
        
        // scheduling_class
        examine_scheduling_class();
        
        // wait_for_completion & cancel
        examine_wait_for_completion();
        examine_cancel_on_interrupt();
        
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
        
        if(process_jvm_args != null) {
            // Tokenize and strip quotes
            ArrayList<String> jvmargs = QuotedOptions.tokenizeList(process_jvm_args, true);
            for(String jvmarg : jvmargs) {
                commandArray.add(jvmarg);
            }
        }

        // Now the AllInOne class and all its legal options
        commandArray.add(AllInOne.class.getCanonicalName());
        for (UiOption opt : allInOneOpts) {
            String val = jobRequestProperties.getProperty(opt.pname());
            if (val != null) {
                commandArray.add("--" + opt.pname());
                if (opt.argname() != null ) {
                    commandArray.add(val);
                }
            }
        }
        
        String[] command = commandArray.toArray(new String[0]);
        ProcessBuilder pb = new ProcessBuilder( command );
        if(working_directory != null) {
            message = "working directory: "+working_directory;
            mh.frameworkDebug(cid, mid, message);
            File wd = new File(working_directory);
            pb.directory(wd);
        }
        
        // Put environment settings in the process's environment
        if(environment != null) {
            ArrayList<String> envList = QuotedOptions.tokenizeList(environment, true); // Strip quotes
            Map<String,String> envMap = QuotedOptions.parseAssignments(envList, false); // Keep all entries
            Map<String,String> env = pb.environment();
            env.clear();
            env.putAll(envMap);
        }
        
        // Dump command line
        for (String arg : commandArray) {
            mh.frameworkDebug(cid, mid, arg);
        }
        
        // Run!
        pb.redirectErrorStream(true);
        Process process = pb.start();
        
        String line;
        
        InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader ibr = new BufferedReader(isr);
        // Pass all process output through the console callback
        while ((line = ibr.readLine()) != null) {
            consoleCb.console(1, line);
        }
        ibr.close();
        try {
            returnCode = process.waitFor();
        } catch (InterruptedException e) {
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
    
    /*
     * Options that AllInOne needs
     */
    UiOption[] allInOneOpts = {
          UiOption.Debug,
          UiOption.Timestamp,
          UiOption.DriverDescriptorCR,
          UiOption.DriverDescriptorCROverrides,
          UiOption.ProcessDD,
          UiOption.ProcessDescriptorCM,
          UiOption.ProcessDescriptorCMOverrides,
          UiOption.ProcessDescriptorAE,
          UiOption.ProcessDescriptorAEOverrides,
          UiOption.ProcessDescriptorCC,
          UiOption.ProcessDescriptorCCOverrides };
    
    /*
     * Create a string hold the args for the java command.
     * If any values contain blanks they would have to be quoted, instead restrict the args
     * to just those needed to run the pipeline.
     */
    private String getProcessExecutableArgs() {
        String mid = "getProcessExecutableArgs";
        mh.frameworkTrace(cid, mid, "enter");
        StringBuffer sb = new StringBuffer();
        if(process_jvm_args != null) {
            sb.append(process_jvm_args);
        }
        sb.append(" -classpath");
        sb.append(" ");
        sb.append(classpath);
        sb.append(" ");
        sb.append(AllInOne.class.getCanonicalName());
        sb.append(" ");
        for (UiOption opt : allInOneOpts) {
            String val = jobRequestProperties.getProperty(opt.pname());
            if (val != null) {
                sb.append(" --" + opt.pname());
                if (opt.argname() != null ) {
                    if (val.indexOf(' ') >= 0) {
                        sb.append(" \"" + val + "\"");
                    } else {
                        sb.append(" " + val);
                    }
                }
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
        addArg(cmdLine, jvm);
        addArg(cmdLine, "--"+UiOption.ProcessExecutableArgs.pname());
        addArg(cmdLine, getProcessExecutableArgs());
        if(scheduling_class != null) {
            addArg(cmdLine, "--"+UiOption.SchedulingClass.pname());
            addArg(cmdLine, scheduling_class);
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
        }
        if(cancel_on_interrupt) {
            addArg(cmdLine, "--"+UiOption.CancelOnInterrupt.pname());
        }
        addArg(cmdLine, "--"+UiOption.AttachConsole.pname());    // Always return console output to match "local"

        String[] argList = cmdLine.toArray(new String[cmdLine.size()]);
        
        DuccManagedReservationSubmit mr = new DuccManagedReservationSubmit(argList, consoleCb);
        boolean rc = mr.execute();
        
        String dt = "Managed Reservation";
        
        if (rc) {
            String line = dt + " " + mr.getDuccId() + " submitted.";
            consoleCb.status(line);
            returnCode = mr.getReturnCode();
        } 
        else {
            String line = "Could not submit " + dt;
            consoleCb.status(line);
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
    
    public boolean execute() throws Exception {
        String mid = "execute";
        mh.frameworkTrace(cid, mid, "enter");
        examine();
        returnCode = -1;  // Some "failure" value in case the local/remote launch doesn't complete
        launch();
        mh.frameworkTrace(cid, mid, "exit");
        return true;
    }
    
    public int getReturnCode() {
      return returnCode;
    }
    
    public static void main(String[] args) throws Exception {
        AllInOneLauncher allInOneLauncher = new AllInOneLauncher(args);
        allInOneLauncher.execute();
        System.exit(allInOneLauncher.getReturnCode());
    }
    

}
