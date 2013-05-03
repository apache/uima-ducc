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
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.uima.ducc.common.IDucc;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.dispatcher.DuccEventHttpDispatcher;
import org.apache.uima.ducc.transport.event.AbstractDuccOrchestratorEvent;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;

public abstract class CliBase
    implements IUiOptions
{

    private String myClassName = "N/A";
    private boolean init_done = false;
    protected String ducc_home;
    protected DuccProperties ducc_properties;
    protected DuccEventHttpDispatcher dispatcher;

    protected Options options;
    protected CommandLineParser parser;
    protected CommandLine commandLine;

    protected long friendlyId = -1;
    protected int  returnCode = 0;

    protected DuccProperties cli_props;
    protected ArrayList<String> errors   = new ArrayList<String>();
    protected ArrayList<String> warnings = new ArrayList<String>();
    protected ArrayList<String> messages = new ArrayList<String>();

    protected boolean debug = false;

    protected abstract boolean execute() throws Exception;

    protected ConsoleListener  console_listener = null;
    protected String host_address = "N/A";
    protected boolean console_attach = false;
    protected IDuccCallback consoleCb = null;

    protected MonitorListener monitor_listener = null;

    CountDownLatch waiter = null;

    String getLogDirectory(String extension)
    {
        /*
         * employ default log directory if not specified
         */
        String log_directory = cli_props.getProperty(UiOption.LogDirectory.pname());
        if(log_directory == null) {
            // no log directory was specified - default to user's home + "/ducc/logs"
            log_directory = System.getProperty("user.home") + IDucc.userLogsSubDirectory;
        } else {
            if(log_directory.startsWith(File.separator)) {
                // absolute log directory was specified
            } else {
                // relative log directory was specified - default to user's home + relative directory
                if(log_directory.endsWith(File.separator)) {
                    log_directory = System.getProperty("user.home") + log_directory;
                }
                else {
                    log_directory = System.getProperty("user.home") + File.separator+log_directory;
                }
            }
        }
        if ( extension != null ) {
            log_directory = log_directory + File.separator + extension;
        }

        cli_props.setProperty(UiOption.LogDirectory.pname(), log_directory);

        /*
         * make sure the logdir is actually legal.
         * JD may also be creating it so to reduce any race or NFS delay blindly create and then test
         */
        File f = new File(log_directory);

        f.mkdirs();
        if ( ! f.exists() ) {
            throw new IllegalArgumentException("getLogDirectory: Cannot create log directory " + log_directory);
        }

        if ( ! f.isDirectory() ) {
            throw new IllegalArgumentException("Specified log_directory is not a directory: " + log_directory);
        }

        if ( ! f.canWrite() ) {
            throw new IllegalArgumentException("Log directory exists but cannot be written: " + f);
        }

        if ( ! f.canExecute() ) {
            throw new IllegalArgumentException("Log directory exists but cannot be accessed (must be writable and executable): " + f);
        }

        return log_directory;
    }

    void setWorkingDirectory()
    {
        String working_directory = cli_props.getProperty(UiOption.WorkingDirectory.pname());
        if(working_directory == null) {
            working_directory = System.getProperty("user.dir");
            cli_props.setProperty(UiOption.WorkingDirectory.pname(), working_directory);
        }
        File f = new File(working_directory);
        if ( ! f.exists() ) {
            throw new IllegalArgumentException("Working directory " + working_directory + " does not exist.");
        }
        if ( ! f.canExecute() ) {
            throw new IllegalArgumentException("Working directory " + working_directory + " exists but cannot be accessed.");
        }
    }

    /*
     * resolve ${defaultBrokerURL} in service dependencies - must fail if resolution needed but can't resolve
     */
    boolean resolve_service_dependencies(String endpoint)
    {
        String key_ja = UiOption.ProcessJvmArgs.pname();
        if ( cli_props.containsKey(UiOption.JvmArgs.pname()) ) {
            key_ja = UiOption.JvmArgs.pname();
        }
        String jvmargs = cli_props.getProperty(key_ja);
        
        Properties jvmprops = DuccUiUtilities.jvmArgsToProperties(jvmargs);

        String deps = cli_props.getProperty(UiOption.ServiceDependency.pname());
        try {
            deps = DuccUiUtilities.resolve_service_dependencies(endpoint, deps, jvmprops);                
            if ( deps != null ) {
                cli_props.setProperty(UiOption.ServiceDependency.pname(), deps);
            }
            return true;
        } catch ( Throwable t ) {
            message("ERROR:", t.toString());
            return false;
        }
    }

    void setUser()
        throws Exception
    {
        /*
         * marshal user
         */
        String user = DuccUiUtilities.getUser();
        cli_props.setProperty(UiOption.User.pname(), user);
        String property = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_signature_required);
        if(property != null) {
            String signatureRequiredProperty = property.trim().toLowerCase();
            if(signatureRequiredProperty.equals("on")) {
                Crypto crypto = new Crypto(System.getProperty("user.home"));
                byte[] cypheredMessage = crypto.encrypt(user);
                cli_props.put(UiOption.Signature.pname(), cypheredMessage);
            }
        }
    }

    /**
     * @param args - array of arguments to the cli parser
     * @param boolean - if true, then add tick to insure required args are present
     */
    protected Options makeOptions(UiOption[] optlist, boolean strict)
    {
        Options opts = new Options();
        for ( UiOption opt : optlist ) {
            String arg = opt.argname();
            Option o = new Option(opt.sname(), opt.pname(), (arg != null), opt.makeDesc());
            o.setArgName(arg);
            o.setOptionalArg(arg != null && arg.endsWith("(optional)"));
            o.setRequired(strict && opt.required());
            if (opt.multiargs()) {
              o.setArgs(Option.UNLIMITED_VALUES);   // (Untested as we have no multiarg options)
            }
            opts.addOption(o);
            // Note: avoid OptionBuilder as is not thread-safe
        }
        return opts;
    }

    protected String[] mkArgs(Properties props)
    {
        List<String> arglist = new ArrayList<String>();
        for ( Object o : props.keySet() ) {
            String k = (String) o;
            String v = props.getProperty(k, "");
            arglist.add("--" + k);
            // Assume an empty value indicates a boolean option
            if (v.length() > 0) {
              arglist.add(v);
            }
        }
        return arglist.toArray(new String[arglist.size()]);
    }
    
    private String[] mkArgs(DuccProperties props)
    {
        List<String> arglist = new ArrayList<String>();
        for ( Object o : props.keySet() ) {
            String k = (String) o;
            String v = props.getStringProperty(k, "");
            arglist.add("--" + k);
            // Assume an empty value indicates a boolean option
            if (v.length() > 0) {
              arglist.add(v);
            }
        }
        return arglist.toArray(new String[arglist.size()]);
    }

    private void enhanceProperties(CommandLine commandLine, boolean showdebug)
    {
        Option[] cliopts = commandLine.getOptions();
        for ( Option o : cliopts ) {
            if ( debug && showdebug ) {
                System.out.println("CLI Override: " + o.toString());
            }
            String k = o.getLongOpt().trim();
            String v = o.getValue();
            if ( v == null ) v = "";      // Boolean options have an empty value
            v = v.trim();

            cli_props.put(k, v);
        }
    }


    /**
     * Use this init if you use the default log location and don't need a console callback.
     */
    protected synchronized void init(String myClassName, UiOption[] opts, String[] args, DuccProperties cli_props, String host_s, String port_s, String servlet)
        throws Exception
    {
        this.init(myClassName, opts, args, cli_props, host_s, port_s, servlet, null, null);
    }

    /**
     * Use this init if you you need a console callback and use the default log location.
     */
    protected synchronized void init(String myClassName, UiOption[] opts, String[] args, DuccProperties cli_props, 
            String host_s, String port_s, String servlet, IDuccCallback consoleCb)
        throws Exception
    {
        this.init(myClassName, opts, args, cli_props, host_s, port_s, servlet, consoleCb, null);
    }

    protected synchronized void init() {
        ducc_home = Utils.findDuccHome();
    }
    
    protected synchronized void init(String myClassName, UiOption[] opts, String[] args, DuccProperties cli_props,
                     String host_s, String port_s, String servlet, IDuccCallback consoleCb, String logExtension)
        throws Exception
    {
        if ( init_done ) return;
        
        if ( consoleCb == null ) {
            this.consoleCb =  new DefaultCallback();
        } else {
            this.consoleCb = consoleCb;
        }

        this.myClassName = myClassName;
        ducc_home = Utils.findDuccHome();

        this.cli_props = cli_props;
        parser = new PosixParser();

        // Set up for reverse lookup
//        for (UiOption opt : UiOption.values() ) {
//            reverseOptions.put(opt.pname(), opt);
//        }

//         options.addOption(OptionBuilder
//                           .withArgName(DuccUiConstants.parm_driver_descriptor_CR)
//                           .withDescription(makeDesc(DuccUiConstants.desc_driver_descriptor_CR,DuccUiConstants.exmp_driver_descriptor_CR)).hasArg()
//                           .withLongOpt(DuccUiConstants.name_driver_descriptor_CR).create());

        // If given only a properties file convert to an array of strings for the parser
        if (args == null) {
            args = mkArgs(cli_props);
            cli_props.clear();
        }
        
        // Initially don't check for required options as they may be in a specification file
        options = makeOptions(opts, false);
        commandLine = parser.parse(options, args);

        if (commandLine.hasOption(UiOption.Help.pname())) {
            usage(null);
        }
        if (commandLine.hasOption(UiOption.Debug.pname())) {
            debug = true;
        }
        if(commandLine.getOptions().length == 0) {
            usage(null);
        }

        // Load the specification file, if given on the command line.  Note that registration
        // bypasses the somewhat redundant --specification kw so we check two options.
        String spec1 =  UiOption.Specification.pname();
        String val = null;
        if ( commandLine.hasOption(spec1) ) {
            val = commandLine.getOptionValue(spec1);
        }
        String spec2 =  UiOption.Register.pname();
        if ( commandLine.hasOption(spec2) ) {
            val = commandLine.getOptionValue(spec2);
        }        
        if ( val != null ) {
            File file = new File(val);
            FileInputStream fis = new FileInputStream(file);
            cli_props.load(fis);

            // Loop through options and enhance / override things from cl options
            enhanceProperties(commandLine, true);

            // Now a trick - we'll rebuild the command line with the props as well as the cli args
            // and reparse strictly.
            args = mkArgs(cli_props);
            cli_props.clear();
        }

        // Even if no specification file provided, re-parse and check for required options
        options = makeOptions(opts, true);
        commandLine = parser.parse(options, args);
        enhanceProperties(commandLine, false);

        String propsfile = ducc_home + "/resources/ducc.properties";
        ducc_properties = new DuccProperties();
        ducc_properties.load(propsfile);
        cli_props.setProperty(UiOption.SubmitPid.pname(), ManagementFactory.getRuntimeMXBean().getName());   // my pid

        String host = ducc_properties.getStringProperty(host_s);
        String port = ducc_properties.getStringProperty(port_s);

        if ( host == null ) {
            throw new IllegalStateException(host_s + " is not set in ducc.properties");
        }
        
        if ( port == null ) {
            throw new IllegalStateException(port_s + " is not set in ducc.properties");
        }
            
        String targetUrl = "http://"+ host + ":" + port + "/" + servlet;
        dispatcher = new DuccEventHttpDispatcher(targetUrl);

        if ( getLogDirectory(logExtension) == null ) {
            throw new IllegalArgumentException("Cannot access log directory.");
        }
        setWorkingDirectory();
        setUser();

        NodeIdentity ni = new NodeIdentity();
        this.host_address = ni.getIp();           

        initConsoleListener();

        init_done = true;
    }

    void saveSpec(String name, DuccProperties props) 
        throws Exception
    {
        String directory = props.getProperty("log_directory") + File.separator + friendlyId;
        String fileName = directory + File.separator + name;
        File f = new File(directory);

        f.mkdirs();
        if ( ! f.exists() ) {
            throw new IllegalStateException("saveSpec: Cannot create log directory: " + f.toString());
        }

        String comments = null;
        FileOutputStream fos = null;
        OutputStreamWriter out = null;
        fos = new FileOutputStream(fileName);
        out = new OutputStreamWriter(fos);

        String key = UiOption.Signature.pname();
        if ( props.containsKey(key) ) {
            Object value = props.remove(key);
            props.store(out, comments);
            props.put(key, value);
        } else {
            props.store(out, comments);
        }

        out.close();
        fos.close();
    }

    void adjustLdLibraryPath(DuccProperties requestProps, String key) 
    {
        String source = "LD_LIBRARY_PATH";
        String target = "DUCC_"+source;
        String environment_string = requestProps.getProperty(key);
        Properties environment_properties = DuccUiUtilities.environmentMap(environment_string);
        if (environment_properties.containsKey(source)) {
            if (environment_properties.containsKey(target)) {
                message("WARN", key, " environment conflict:", target, "takes precedence over", source);
            } else {
                target += "="+environment_properties.getProperty(source);
                environment_string += " "+target;
                requestProps.setProperty(key, environment_string);
            }
        }
    }

    /**
     * Extract messages and job pid from reply.  This sets messages and errors into the appropriate
     * structures for the API, and extracts the numeric id of the [job, ducclet, reservation, service]
     * returned by the Orchestrator.
     *
     * @returns true if the action succeeded and false otherwise.  The action in this case, is whatever
     *               the Orchestrator was asked to do: submit something, cancel something, etc.
     */
    boolean extractReply(AbstractDuccOrchestratorEvent reply)
    {
        /*
         * process reply
         */
        boolean rc = true;
        Properties properties = reply.getProperties();
        @SuppressWarnings("unchecked")
        ArrayList<String> value_submit_warnings = (ArrayList<String>) properties.get(UiOption.SubmitWarnings.pname());
        if(value_submit_warnings != null) {
            message("Job warnings:");
            Iterator<String> reasons = value_submit_warnings.iterator();
            while(reasons.hasNext()) {
                message("WARN:", reasons.next());
            }
        }
        @SuppressWarnings("unchecked")
        ArrayList<String> value_submit_errors = (ArrayList<String>) properties.get(UiOption.SubmitErrors.pname());
        if(value_submit_errors != null) {
            message("Job errors:");
            Iterator<String> reasons = value_submit_errors.iterator();
            while(reasons.hasNext()) {
                message("ERROR:", reasons.next());
            }
            rc = false;
        }

        String pid =  reply.getProperties().getProperty(UiOption.JobId.pname());
        if (pid == null ) {
            rc = false;
        } else {
            friendlyId = Long.parseLong(pid);
            if ( friendlyId < 0 ) {
                rc = false;
            }
        }

        return rc;
    }

    void usage(String message)
    {
        if ( message != null ) {
            System.out.println(message);
        }
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(DuccUiConstants.help_width);
        formatter.printHelp(myClassName, options);
        System.exit(1);
    }

    /**
     * Set a property via the API. 
     *
     * @param key This is the property name.
     * @param value This is the value of the property.
     * @param props This is the Properties objct to update.
     *
     * @returns true if the property is set.  Returns false if the property is not legal for this API.
     */
    public boolean setProperty(String key, String value)
    {

        if ( key.startsWith("--") ) {
            key = key.substring(2);
        }
        Option option = options.getOption(key);
        if (option == null ) {
            return false;
        }
        cli_props.setProperty(key, value);
        return true;
    }

    public boolean isDebug()
    {
        return debug;
    }

    public void setDebug(boolean val)
    {
        this.debug = val;
    }

    public String getHostAddress()
    {
        return host_address;
    }

    public boolean hasProperty(String key)
    {
        return cli_props.containsKey(key);
    }
    
    public String getProperty(String key)
    {
        return (String) cli_props.getProperty(key);
    }

    protected IDuccCallback getCallback()
    {
        return consoleCb;
    }

    /**
     * NOTE: We do NOT want to be intentionally throwing from the CLI.  Pls pass e.getMessage() or
     *       e.toString() to this instead of throwing.
     */
    synchronized void message(String ... e )
    {
        if ( e.length > 1 ) {
            StringBuffer sb = new StringBuffer();
            int i = 0;
            for (i = 0; i < e.length - 1; i++) {
                sb.append(e[i]);
                sb.append(' ');
            }
            sb.append(e[i]);
            consoleCb.status(sb.toString());
        } else {
            consoleCb.status(e[0]);
        }
    }

    /*
     * Return code is only available when the monitor wait completes ... if not waiting then assume success
     */
    public int getReturnCode()
    {
        waitForCompletion();
        return returnCode;
    }

    synchronized public long getDuccId()
    {
        return friendlyId;
    }

    synchronized void consoleExits()
    {
        if ( waiter != null ) waiter.countDown();
    }

    synchronized void monitorExits(int rc)
    {
        this.returnCode = rc;
        if ( waiter != null ) waiter.countDown();
        if ( console_listener != null ) {
            console_listener.shutdown();
        }
    }

    // TODO TODO TODO - do we have to support lots of these for multi-threaded stuff?  Hope not ...
    protected synchronized void startMonitors(boolean start_stdin, DuccContext context)
        throws Exception
    {
        int wait_count = 0;

        if ( console_listener != null ) {
            wait_count++;
        }
        
        boolean monitor_attach = 
                (
                cli_props.containsKey(UiOption.WaitForCompletion.pname()) || 
                cli_props.containsKey(UiOption.CancelOnInterrupt.pname()) || 
                cli_props.containsKey(UiOption.CancelJobOnInterrupt.pname()) || 
                cli_props.containsKey(UiOption.CancelManagedReservationOnInterrupt.pname()) 
                );
            
        if ( monitor_attach ) {
            wait_count++;
        }

        // Probably over-cautious but create the waiter before starting the threads that use it
        if ( wait_count > 0 ) {
            waiter = new CountDownLatch(wait_count);
            if ( console_listener != null ) {
                startConsoleListener(start_stdin);
            }
            if ( monitor_attach ) {
                startMonitor(context);
            }
        }
    }

    protected synchronized void startMonitor(DuccContext context)
    {
        monitor_listener = new MonitorListener(this, friendlyId, cli_props, context);
        Thread mlt = new Thread(monitor_listener);  //MonitorListenerThread
        mlt.start();
    }

    /**
     * Needs to be done before submitting the job because the job needs the ports.  We'll
     * just define the listener, but not start it untile the job monitor starts, in case the
     * submission fails.
     */
    protected void initConsoleListener()
        throws Exception
    {
        console_attach =
            cli_props.containsKey(UiOption.ProcessAttachConsole.pname()) ||
            cli_props.containsKey(UiOption.DriverAttachConsole.pname());

        if ( console_attach ) {
            console_listener = new ConsoleListener(this, consoleCb);
            
            String key_pe = UiOption.ProcessEnvironment.pname();
            String key_de = UiOption.DriverEnvironment.pname();
            if ( cli_props.containsKey(UiOption.Environment.pname()) ) {
                key_pe = UiOption.Environment.pname();
                key_de = UiOption.Environment.pname();
            }
            
            if ( cli_props.containsKey(UiOption.ProcessAttachConsole.pname()) ) {
                set_console_port(cli_props, key_pe);
            } 
            
            if  (cli_props.containsKey(UiOption.DriverAttachConsole.pname()) ) {
                set_console_port(cli_props, key_de);
            } 
        }
    }

    /**
     * Be sure to call this BEFORE submission, to insure the callback address is set in properties.
     */
    protected synchronized void startConsoleListener(boolean start_stdin)
        throws Exception
    {        
        if ( console_attach ) {
            console_listener.startStdin(start_stdin);
            Thread t = new Thread(console_listener);
            t.start();
        } else {
            message("WARN: Attermpt to start console but no console listener is defined.");
        }
    }

    protected void stopListeners()
    {
        if ( console_listener != null ) {
            console_listener.shutdown();
            console_listener = null;
        }

        if ( monitor_listener != null ) {
            monitor_listener.shutdown();
            monitor_listener = null;
        }
    }

    protected void set_console_port(DuccProperties props, String key)
    {
        if ( key != null ) {         
            if ( console_listener == null ) {
                message("WARN: Attempt to set console port but listener is not running.");
                return;
            }

            String console_host_address = console_listener.getConsoleHostAddress();
            int console_listener_port = console_listener.getConsolePort();

            String envval = "DUCC_CONSOLE_LISTENER";
            String env = props.getProperty(key);            
            // Set the host:port for the console listener into the env
            String console_address = console_host_address + ":" + console_listener_port;
            String dp = envval + "=" + console_address;
            if ( env == null ) {
                env = dp;
            } else {
                env = env + " " + dp;
            }
            props.setProperty(key, env);
        }
    }

    public boolean isConsoleAttached()
    {
        return ( (console_listener != null ) && ( !console_listener.isShutdown()));
    }

    /**
     * Wait for the listeners - maybe a console listener, maybe a job listener.
     * @returns true if a monitor wait was done, false otherwise.  A monitor wait
     *          results in a return code from the process.  In all other cases
     *          the return code is spurious.
     */
    public boolean waitForCompletion()
    {
        try {
            if ( waiter != null ) {
                waiter.await();
                return true;
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

}
