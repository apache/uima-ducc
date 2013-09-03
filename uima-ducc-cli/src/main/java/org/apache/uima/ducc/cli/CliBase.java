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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
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

/**
 * Define common methods and data used by all the DUCC API and CLI.
 */
public abstract class CliBase
    implements IUiOptions
{

    private String myClassName = "N/A";
    private boolean init_done = false;
    protected String ducc_home;
    DuccEventHttpDispatcher dispatcher;

    protected Options cliOptions;
    protected Parser parser;
    protected CommandLine commandLine;

    protected long friendlyId = -1;
    protected int  returnCode = 0;

    protected DuccProperties cli_props;
    protected ArrayList<String> errors   = new ArrayList<String>();
    protected ArrayList<String> warnings = new ArrayList<String>();
    protected ArrayList<String> messages = new ArrayList<String>();

    protected boolean debug;

    protected ConsoleListener  console_listener = null;
    protected String host_address = "N/A";
    protected boolean console_attach = false;
    protected IDuccCallback consoleCb = null;

    protected MonitorListener monitor_listener = null;

    CountDownLatch waiter = null;

    protected Properties userSpecifiedProperties;
    
    // Options added to the saved spec file that must be removed if used as a --specification option
    private List<String> addedOptions = Arrays.asList(UiOption.SubmitPid.pname(), UiOption.User.pname());
    
    /**
     * All extenders must implement execute - this method does whatever processing on the input
     * is needed and passes the CLI request to the internal DUCC processes.
     *
     * @return Return true if execution works, and false otherwise.
     * @throws java.lang.Exception The specific exception is a function of the implementor.
     */
    public abstract boolean execute() throws Exception;

    /*
     * Get log directory or employ default log directory if not specified
     */
    String getLogDirectory()
    {

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
     * Check the syntax & if a service refers to itself -- place-holders already resolved
     */
    boolean check_service_dependencies(String endpoint)
    {
        String deps = cli_props.getProperty(UiOption.ServiceDependency.pname());
        try {
            DuccUiUtilities.check_service_dependencies(endpoint, deps);                
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

    /*
     * Also used by DuccMonitor
     */
    static public Options makeOptions(UiOption[] optlist)
    {
        Options opts = new Options();
        for ( UiOption opt : optlist ) {
            String arg = opt.argname();
            Option o = new Option(opt.sname(), opt.pname(), (arg != null), opt.makeDesc());
            o.setArgName(arg);
            o.setOptionalArg(arg != null && arg.endsWith("(optional)"));
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
    
    /**
     * Standard init for all except the Service calls that are sent to the SM
     */

    protected synchronized void init(String myClassName, UiOption[] opts, String[] args, DuccProperties cli_props, 
                    IDuccCallback consoleCb) throws Exception {
        this.init (myClassName, opts, args, null, cli_props, consoleCb, "orchestrator");
    }

    protected synchronized void init(String myClassName, UiOption[] opts, Properties props, DuccProperties cli_props, 
                    IDuccCallback consoleCb) throws Exception {
        this.init (myClassName, opts, null, props, cli_props, consoleCb, "orchestrator");
    }
    
    protected synchronized void init(String myClassName, UiOption[] uiOpts, String[] args, Properties props, 
                    DuccProperties cli_props, IDuccCallback consoleCb, String servlet)
        throws Exception
    {
        // Optionally convert deprecated options
        if (System.getenv("DUCC_ACCEPT_DEPRECATED_OPTIONS") != null) cleanupArgs(args);
        
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

        cliOptions = makeOptions(uiOpts);
        // If given only a properties file parse as if only have defaults
        if (args == null) {
            commandLine = parser.parse(cliOptions, null, props);
        } else {
            fixupQuotedArgs(args);
            commandLine = parser.parse(cliOptions, args);
        }
        if (commandLine.getOptions().length == 0 || commandLine.hasOption(UiOption.Help.pname())) {
            usage(null);
        }
        debug = commandLine.hasOption(UiOption.Debug.pname());

        // Load the specification file, if given on the command line.  Note that registration
        // bypasses the somewhat redundant --specification option so we check two options.
        // Cannot have both as --specification && --register are never both valid.
        String fname = null;
        for (String spec : new String[] { UiOption.Specification.pname(), UiOption.Register.pname()}) {
            fname = commandLine.getOptionValue(spec);
            if (fname != null) break;
        }
        // If have a specification file re-parse using it for default values
        if ( fname != null ) {
            FileInputStream fis = new FileInputStream(new File(fname));
            Properties defaults = new Properties();
            defaults.load(fis);
            fis.close();
            sanitize(defaults, cliOptions);  // Check for illegals as commons cli 1.2 thows a NPE !
            // If invoked with overriding properties add to or replace the defaults 
            if (props != null) {
                defaults.putAll(props);
            }
            commandLine = parser.parse(cliOptions, args, defaults);
        }

        // Copy options into cli_props
        setOptions(uiOpts);
        
        // Save a copy of the user-specified ones by cloning the underlying properties
        userSpecifiedProperties = (Properties)((Properties)cli_props).clone();
        
        cli_props.setProperty(UiOption.SubmitPid.pname(), ManagementFactory.getRuntimeMXBean().getName());

        if ( getLogDirectory() == null ) {
            throw new IllegalArgumentException("Cannot access log directory.");
        }
        setWorkingDirectory();
        setUser();

        //TODO - shouldn't environment fixups be done here for all requests that may use it??
        
        NodeIdentity ni = new NodeIdentity();
        host_address = ni.getIp();           

        initConsoleListener();

        String targetUrl = DuccUiUtilities.dispatchUrl(servlet);
        dispatcher = new DuccEventHttpDispatcher(targetUrl);
        
        init_done = true;
    }

    /*
     * Save options as properties after resolving any ${..} placeholders
     * Also check that all required ones provided
     */
    void setOptions(UiOption[] uiOpts) throws Exception {
        for (Option opt : commandLine.getOptions()) {
            String val = opt.getValue();
            if (val == null) {
                val = opt.hasArg() ? "" : "true"; // Treat no-arg options as booleans ... apache.commons.cli expects this
            } else {
                if (val.contains("${")) {
                    val = resolvePlaceholders(val);
                }
            }
            cli_props.put(opt.getLongOpt(), val);
            if (debug) System.out.println("CLI set " + opt.getLongOpt() + " = " + val);
        }

        for (UiOption opt : uiOpts) {
            if (opt.required() && !cli_props.containsKey(opt.pname())) {
                throw new Exception("Missing required option: " + opt.pname());
            }
        }
    }
    
    /*
     * Clean up the properties in a specification file 
     * Remove any added by the CLI that the parse would call illegal
     * Check for invalid options as Commons CLI 1.2 throws a NPE
     * Correct booleans by treating empty as "true" and removing anything
     * other than 'true' or 'yes' or '1' (CLI 1.2 mishandles others)
     */
    
    private void sanitize(Properties props, Options opts) {
        for (String key : props.stringPropertyNames()) {
            if (addedOptions.contains(key)) {
                props.remove(key);
            } else {
                Option opt = cliOptions.getOption(key);
                if (opt == null) {
                    throw new IllegalArgumentException("Invalid option " + key + " in specification file");
                }
                if (!opt.hasArg()) {
                    String val = props.getProperty(key);
                    if (val.length() == 0) {
                        props.setProperty(key, "true");
                    } else if (!val.equalsIgnoreCase("true") &&
                               !val.equalsIgnoreCase("yes") &&
                               !val.equals("1")) {
                        message("WARN: Ignoring illegal value: ", key, "=", val);
                        props.remove(key);
                    }
                }
            }
        }
    }
    
    /*
     * Resolve any ${..} placeholders against user's system properties and environment
     */
    private String resolvePlaceholders(String contents) {
        //  Placeholders syntax ${<placeholder>} 
        Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");  // Stops on first '}'
        Matcher matcher = pattern.matcher(contents); 

        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            final String key = matcher.group(1);
            String value = System.getProperty(key);
            if (value == null) {
                value = System.getenv(key);
                if (value == null) {
                    throw new IllegalArgumentException("Missing value for placeholder '" + key + "' in: " + contents);
                }
            }
            matcher.appendReplacement(sb, value);        
        }
        matcher.appendTail(sb);
        return sb.toString();
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

        // Save the specification (but exclude the 'signature' entry)
        String comments = null;
        OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(fileName));
        String key = UiOption.Signature.pname();
        if ( props.containsKey(key) ) {
            Object value = props.remove(key);
            props.store(out, comments);
            props.put(key, value);
        } else {
            props.store(out, comments);
        }
        out.close();
        
        // Also save just the values the user provided
        fileName = directory + File.separator + DuccUiConstants.user_specified_properties;
        out = new OutputStreamWriter(new FileOutputStream(fileName));
        userSpecifiedProperties.store(out, comments);
        out.close();
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
            message("ERROR: JobId not found in reply");
            rc = false;
        } else {
            friendlyId = Long.parseLong(pid);
            if ( friendlyId < 0 ) {
                message("ERROR: Invalid JobId", pid);
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
        formatter.printHelp(myClassName, cliOptions);
        System.exit(1);
    }

    /**
     * Set a property via the API. This method allows the API user to
     * build up or override properties after the initial API object is constructed.
     *
     * @param key This is the property name.
     * @param value This is the value of the property.
     *
     * @return true if the property is set.  Returns false if the property is not legal for this API.
     */
    public boolean setProperty(String key, String value)
    {

        if ( key.startsWith("--") ) {
            key = key.substring(2);
        }
        Option option = cliOptions.getOption(key);
        if (option == null ) {
            return false;
        }
        cli_props.setProperty(key, value);
        return true;
    }

    /**
     * Return internal API debug status.
     * @return True if the API debugging flag is set; false otherwise.
     */
/*    public boolean isDebug()
    {
        return debug;
    }*/

    /**
     * Set the internal API debug flag.
     * @param val Set to true to enable debugging, and false to disable it.
     */
/*    public void setDebug(boolean val)
    {
        this.debug = val;
    }
*/
    // nobody seems to use this
//     public String getHostAddress()
//     {
//         return host_address;
//     }

/* Also unused?
 *     public boolean hasProperty(String key)
    {
        return cli_props.containsKey(key);
    }
    
    public String getProperty(String key)
    {
        return (String) cli_props.getProperty(key);
    }*/

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

    /**
     * This returns the return code from the execution of the requested work.  Return code is only
     * available when the monitor wait completes ... if not waiting then assume success.
     *
     * @return The exit code of the job, process, etc.
     */
    public int getReturnCode()
    {
        waitForCompletion();
        return returnCode;
    }

    /**
     * This returns the unique numeric id for the requested work.  For submissions (job, reservation, etc)
     * this is the newly assigned id.
     * @return The unique numeric id of the job, reservation, etc.
     */
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
                cli_props.containsKey(UiOption.CancelOnInterrupt.pname())  
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
            
            String key = UiOption.Environment.pname();
            
            if ( cli_props.containsKey(UiOption.ProcessAttachConsole.pname()) ) {
                set_console_port(cli_props, key);
            } 
            
            if  (cli_props.containsKey(UiOption.DriverAttachConsole.pname()) ) {
                set_console_port(cli_props, key);
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

    protected synchronized void stopListeners()
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

    /**
     * This is used to find if the remote console is redirected to the local process, and if so, is it still
     * active.
     * @return True if the console is still attached to the remote process, false otherwise.
     */
    public boolean isConsoleAttached()
    {
        return ( (console_listener != null ) && ( !console_listener.isShutdown()));
    }

    /**
     * Wait for the listeners - maybe a console listener, maybe a job listener, maybe both.
     *
     * @return true if a monitor wait was done, false otherwise.  A monitor wait
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

    /*
     * Since apache-commons-cli 1.2 wrongly removes initial or final quotes, add extra one(s)
     * i.e. an --environment setting of FOO="a b" becomes FOO="a b""
     * What about a lonely " ... both starts & ends so would become => """
     */
    private String[] fixupQuotedArgs(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].charAt(0) == '"') {
                args[i] = "\"" + args[i];
            }
            if (args[i].endsWith("\"")) {
                args[i] = args[i] + "\"";
            }
        }
        return args;
    }
    
    /*
     * Get specified class path (or the default) and remove any DUCC jars used to submit a request
     * so they cannot accidentally replace any in the user's classpath.
     */
    protected String fixupClasspath(String key_cp) {
        String classpath = cli_props.getStringProperty(key_cp,
                        System.getProperty("java.class.path"));
        StringBuilder sb = new StringBuilder();
        String duccPrefix = ducc_home + "/lib";
        for (String jar : classpath.split(":")) {
            if (!jar.startsWith(duccPrefix)) {
                sb.append(jar).append(":");
            }
        }
        classpath = sb.toString();
        cli_props.setProperty(key_cp, classpath);
        return classpath;
    }
    
    /*
     * Change any deprecated options to their new name.  If this produces duplicate specifications then the 
     * commons-cli parser probably chooses the last one.
     * 
     */
    private void cleanupArgs(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            String arg = args[i];
            if (arg.equals("--driver_classpath") || arg.equals("--process_classpath")) {
                args[i] = "--classpath";
            } else if (arg.equals("--driver_environment") || arg.equals("--process_environment")) {
                args[i] = "--environment";
            } else if (arg.equals("--cancel_job_on_interrupt") || arg.equals("--cancel_managed_reservation_on_interrupt")) {
                args[i] = "--cancel_on_interrupt";
            } else if (arg.equals("--jvm_args")) {
                args[i] = "--process_jvm_args";
            }
        }
    }
}
