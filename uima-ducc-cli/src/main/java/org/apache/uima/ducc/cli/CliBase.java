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
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.dispatcher.IDuccEventDispatcher;
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
    protected IDuccEventDispatcher dispatcher;

    protected CommandLine commandLine;

    protected long friendlyId = -1;
    protected int  returnCode = 0;

    protected DuccProperties cli_props;
    protected ArrayList<String> errors   = new ArrayList<String>();
    protected ArrayList<String> warnings = new ArrayList<String>();
    protected ArrayList<String> messages = new ArrayList<String>();

    protected boolean debug;
    private   boolean load_defaults = true;

    protected ConsoleListener  console_listener = null;
    protected boolean suppress_console_log;
    protected String host_address = "N/A";
    protected boolean console_attach = false;
    protected IDuccCallback consoleCb = null;

    protected MonitorListener monitor_listener = null;

    CountDownLatch waiter = null;

    protected Properties userSpecifiedProperties;

    /**
     * All extenders must implement execute - this method does whatever processing on the input
     * is needed and passes the CLI request to the internal DUCC processes.
     *
     * @return Return true if execution works, and false otherwise.
     * @throws java.lang.Exception The specific exception is a function of the implementor.
     */
    public abstract boolean execute() throws Exception;

    protected void inhibitDefaults()
    {
        this.load_defaults = false;
    }

    /*
     * Make the log directory absolute if necessary and check that it is usable.
     * If not provided it will have been given a default value.
     * UIMA-4617 Make it relative to the run-time working directory, not HOME
     */
    String getLogDirectory(String working_directory) throws IOException {
        String log_directory = cli_props.getProperty(UiOption.LogDirectory.pname());
        File f;
        if (log_directory.startsWith(File.separator)) {
            f = new File(log_directory);
        } else {
            f = new File(working_directory, log_directory);   // Relative to working directory
            log_directory = f.getCanonicalPath();
            cli_props.setProperty(UiOption.LogDirectory.pname(), log_directory);
        }

        /*
         * make sure the logdir is actually legal.
         * JD may also be creating it so to reduce any race or NFS delay blindly create and then test
         */
        f.mkdirs();
        if ( ! f.isDirectory() || ! f.canWrite()) {
            throw new IllegalArgumentException("Specified log_directory is not a writable directory: " + log_directory);
        }

        return log_directory;
    }

    /*
     * Ensure the working directory exists and is absolute
     */
    String getWorkingDirectory() throws IOException {
        String working_directory = cli_props.getProperty(UiOption.WorkingDirectory.pname());
        File f = new File(working_directory);
        if ( ! f.exists() ) {
            throw new IllegalArgumentException("Working directory " + working_directory + " does not exist.");
        }
        if ( ! f.isAbsolute() ) {
          working_directory = f.getCanonicalPath();
          cli_props.setProperty(UiOption.WorkingDirectory.pname(), working_directory);
        }
        return working_directory;
    }

    /*
     * Check the syntax & if a service refers to itself -- place-holders already resolved
     * Strip any broker URL decorations
     */
    boolean check_service_dependencies(String endpoint)
    {
        String deps = cli_props.getProperty(UiOption.ServiceDependency.pname());
        try {
            String dependencies = DuccUiUtilities.check_service_dependencies(endpoint, deps);
            if (dependencies != null) {
                cli_props.setProperty(UiOption.ServiceDependency.pname(), dependencies);
            }
            return true;
        } catch ( Throwable t ) {
            message("ERROR:", t.toString());
            return false;
        }
    }

    /*
     * Check if -Xmx value is >= memory size ... if both are specified
     */
    void check_heap_size(String argsOption) {
        String jvmArgs = cli_props.getProperty(argsOption);
        String memSize = cli_props.getProperty(UiOption.ProcessMemorySize.pname());
        if (jvmArgs == null || memSize == null) {
            return;
        }

        // The numbers may be terminated by a units factor, white-space, or the end of the string
        // The units factor may be any of kKmMgG ... if omitted is bytes
        // Match -Xmx###[units-flag] and take the last one specified (IBM & Oracle JREs do this)
        String xmxRegex = "-Xmx([0-9]+)($|[\\skKmMgG])";
        Pattern patn = Pattern.compile(xmxRegex);
        Matcher matcher = patn.matcher(jvmArgs);
        Long size = null;
        String unit = null;
        while (matcher.find()) {
            size = Long.valueOf(matcher.group(1));
            unit = matcher.group(2);
        }
        if (size == null) {
            return;
        }
        if (unit.isEmpty()) { // Was last option in list
            unit = " ";
        }
        char factor = unit.toLowerCase().charAt(0);
        int shift = "gmk".indexOf(factor); // Number of 1024's to divide size by to get GB
        if (shift < 0) {    // No explicit unit factor ... white-space => bytes
            shift = 3;
        }
        long sizeGB = size >> (10 * shift); // Shift 10 bits per K
        int memGB = Integer.valueOf(memSize);
        if (sizeGB >= memGB) {
            String text = "WARNING - process_memory_size is " + memSize + "G but the max heap is " + size + unit + " --- swapping may occur";
            message(text);
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
                Crypto crypto = new Crypto(user, true);
                byte[] cypheredMessage = crypto.getSignature();
                cli_props.put(UiOption.Signature.pname(), cypheredMessage);
            }
        }
    }

    /*
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

  /**
   *
   * @param myClassName  Name of the class invoking me, for help string
   * @param uiOpts       Array of IUioptions permitted for this command
   * @param args         Arguments from the command line (or null)
   * @param props        Properties passed in from the API (or null)
   * @param cli_props    (Initially) empty properties file to be filled in
   * @param consoleCb    Console callback object (optional)
   * @param servlet      The name of the http servlet that will serve this request
   * @throws Exception   If initialization fails, e.g. invalid arguments or properties
   */
    protected synchronized void init(String myClassName, IUiOption[] uiOpts, String[] args, Properties props,
                    DuccProperties cli_props, IDuccCallback consoleCb, String servlet)
        throws Exception
    {

    	// Either args or props passed in, not both
    	if (args != null) {
    		CliFixups.cleanupArgs(args, myClassName);
    	} else {
    		CliFixups.cleanupProps(props, myClassName);
    	}

        if ( init_done ) return;

        if ( consoleCb == null ) {
            this.consoleCb =  new DefaultCallback();
        } else {
            this.consoleCb = consoleCb;
        }

        this.myClassName = myClassName;
        ducc_home = Utils.findDuccHome();

        this.cli_props = cli_props;
        commandLine = new CommandLine(args, uiOpts, props);
        try {
			commandLine.parse();
		} catch (Exception e) {
			usage(e.getMessage());
		}

        if ( commandLine.contains(UiOption.Help)) {
        	usage(null);
        }

        debug = commandLine.contains(UiOption.Debug);

        // Load the specification file, if given on the command line.  Note that registration
        // bypasses the somewhat redundant --specification option so we check two options.
        // Cannot have both as --specification && --register are never both valid.
        String fname = null;
        for (IUiOption spec : new IUiOption[]{ UiOption.Specification, UiOption.Register }) {
            if ( commandLine.isOption(spec) && commandLine.contains(spec)) {     // legal for this command, and also specified?
            	fname = commandLine.get(spec);
            	if (fname.length() == 0) {		// Check if --register has no value
            		fname = null;
            	}
            	break;
             }
        }
        // If have a specification file re-parse using it for default values
        if ( fname != null ) {
            FileInputStream fis = new FileInputStream(new File(fname));
            Properties defaults = new Properties();
            defaults.load(fis);
            fis.close();
            CliFixups.cleanupProps(defaults, myClassName);     // May correct or drop deprecated options

            // If invoked with overriding properties add to or replace the defaults
            if (props != null) {
                defaults.putAll(props);
            }
            commandLine = new CommandLine(args, uiOpts, defaults);
            commandLine.parse();
        }
        commandLine.verify();  // Insure all the rules specified by the IUiOpts are enforced

        // Copy options into cli_props
        setOptions(uiOpts);

        // Save a copy of the user-specified ones by cloning the underlying properties
        userSpecifiedProperties = (Properties)((Properties)cli_props).clone();

        // May need to suppress logging in console listener, or in the DUCC process.
        suppress_console_log = cli_props.containsKey(UiOption.SuppressConsoleLog.pname());

        // This is not used by DUCC ... allows ducc-mon to display the origin of a job
        cli_props.setProperty(UiOption.SubmitPid.pname(), ManagementFactory.getRuntimeMXBean().getName());

        // Apply defaults for and fixup the environment if needed
        //   -- unless default loading is inhibited, as it must be for modify operations
        //      What this routine does is fill in all the options that weren't specified
        //      on the command line with their defaults.  For 'modify' we want to bypass
        //      this because ONLY the options from the command line should be set.
        //      So modify must not change the log directory.
        if ( load_defaults ) {
            setDefaults(uiOpts, suppress_console_log);
        }
        setUser();

        //NodeIdentity ni = new NodeIdentity(); UIMA-3899, use getHostAddress() directly.  jrc
        host_address = InetAddress.getLocalHost().getHostAddress();

        initConsoleListener();

        // AllInOne doesn't dispatch requests (and local doesn't need a running DUCC!)
        if (!cli_props.containsKey(UiOption.AllInOne.pname())) {
            dispatcher = DispatcherFactory.create(cli_props, servlet);
        }

        init_done = true;
    }

    /*
     * Save options as properties after resolving any ${..} placeholders
     */
    void setOptions(IUiOption[] uiOpts)
        throws Exception
    {
        // Find the environment variables that are always propagated
        List<String> envNameList;
        String envNames = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_environment_propagated);
        if (envNames != null) {
        	envNameList = Arrays.asList(envNames.split("\\s+"));
        } else {
        	envNameList = new ArrayList<String>(0);
        }

    	Map<IUiOption, String> parsed = commandLine.allOptions();
        for (IUiOption opt : parsed.keySet() ) {
        	// If a "flexible" boolean that accepts various true/false values, add it only if true
        	String val;
        	if (opt.optargs() && "true".equals(opt.deflt())) {
        		boolean bval = commandLine.getBoolean(opt);
        		if (bval) {
        			val = "";
        		} else {
        			if (debug) System.out.println("CLI omitted boolean " + opt.pname() + " = '" + parsed.get(opt) + "'");
        			continue;
        		}
			} else {
				val = parsed.get(opt);
				if (val == null) { // Should only happen for no-arg options
					val = "";
				} else {
					if (val.contains("${")) {
						val = resolvePlaceholders(val, envNameList);
					}
				}
			}
            val = val.trim();
            cli_props.put(opt.pname(), val);
            if (debug) System.out.println("CLI set " + opt.pname() + " = '" + val + "'");
        }
    }

    /*
     * Check for missing required options, set defaults, and validate where possible
     * Also fixup the environment for all that use it.
     */
    void setDefaults(IUiOption[] uiOpts, boolean suppress_console) throws IOException {
        String logDir = null, workingDir = null;
        ArrayList<String> envNameList = new ArrayList<String>(0);   // Why this when are resolving against use caller's environment?
        for (IUiOption uiopt : uiOpts) {
            if (!cli_props.containsKey(uiopt.pname())) {
                //
                // here deal with stuff that wasn't given explicitly in the command
                //
                // our convention - optargs() implies boolean, but it does't have to.
                //                  If the arg is not expllicitly specified, we assume
                //                  it is (boolean,false) for the sake of dealing with defaults.
                //                  -- and then just leave it out --
                // similarly      - noargs() is definitely boolean, same treatement
                //
                if ( (! uiopt.optargs()) && (! uiopt.noargs() ) && uiopt.deflt() != null) {
                    String deflt = uiopt.deflt();
                    if (deflt.startsWith("$$")) {     // Lookup default in ducc.properties
                        deflt = DuccPropertiesResolver.get(deflt.substring(2));
                        if (deflt == null) {
                            throw new IllegalArgumentException("Invalid default (undefined property) for " + uiopt.pname());
                        }
                    } else if (deflt.contains("${")) {
                        deflt = resolvePlaceholders(deflt, envNameList);
                    }
                    if (debug) System.out.println("CLI set default: " + uiopt.pname() + " = " + deflt);
                    cli_props.put(uiopt.pname(), deflt);
                }
            } else {
                //
                // here clean up stuff that was specified but we want to validate it
                //
                if (uiopt == UiOption.ProcessMemorySize || uiopt == UiOption.ReservationMemorySize)  {
                    String val = cli_props.getStringProperty(uiopt.pname());
                    if (!val.matches("^\\d+$")) {
                        throw new IllegalArgumentException("Invalid non-numeric value for " + uiopt.pname() + ": " + val);
                    }
                }
            }
            // NOTE: These 3 options must be in this order so each depends on the previous
            if (uiopt == UiOption.WorkingDirectory) {
              workingDir = getWorkingDirectory();
            } else if (uiopt == UiOption.LogDirectory) {
              logDir = getLogDirectory(workingDir);
            } else if (uiopt == UiOption.Environment) {
              // If this request accepts the --environment option may need to augment it by
              // renaming LD_LIBRARY_PATH & propagating some user values
              // Pass in the log directory so DUCC_UMASK may be set.  UIMA-5328
              String environment = cli_props.getProperty(uiopt.pname());
              String allInOne = cli_props.getProperty(UiOption.AllInOne.pname());
              environment = DuccUiUtilities.fixupEnvironment(environment, allInOne, logDir);
              cli_props.setProperty(uiopt.pname(), environment);
            }
        }
    }

    /*
     * Resolve any ${..} placeholders against user's system properties and environment
     * NOTE - this resolves against the caller's sys-props & environment ... the one in DuccUiUtilities
     *        resolves against the process JVM args to match what is done by Spring in UIMA-AS.
     * 2.0: Leave unresolved entries as is & warn if not one of the always-propagated ones
     */
    private String resolvePlaceholders(String contents, List<String> envNameList) {
        //  Placeholders syntax ${<placeholder>}
        Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");  // Stops on first '}'
        Matcher matcher = pattern.matcher(contents);

        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            final String key = matcher.group(1);
            String value = System.getProperty(key);
            if (value == null) {
                value = System.getenv(key);
            }
            if (value != null) {
            	matcher.appendReplacement(sb, value);
            } else {
            	matcher.appendReplacement(sb, "");   // Can't include the value as it looks like a group specification
            	value = "${" + key + "}";
            	sb.append(value);
            	if (!envNameList.contains(key)) {
            		message("WARN: undefined placeholder", value, "not replaced");
            	}
            }
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
     * @param reply - an Orchestrator reply event
     *   
     * @return true if the action succeeded and false otherwise.  The action in this case, is whatever
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
            message("ERROR: Request ID not found in reply");
            rc = false;
        } else {
            friendlyId = Long.parseLong(pid);
            if ( friendlyId < 0 ) {
                message("ERROR: Invalid Request ID", pid);
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
        System.out.println(commandLine.formatHelp(myClassName));
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

        if ( ! commandLine.isOptionName(key)) {
            return false;
        }
        cli_props.setProperty(key, value);
        return true;
    }

    protected IDuccCallback getCallback()
    {
        return consoleCb;
    }

    /*
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

    /*
     * Needs to be done before submitting the job because the job needs the ports.  We'll
     * just define the listener, but not start it until the job monitor starts, in case the
     * submission fails.
     */
    protected void initConsoleListener() throws Exception {
        String value;

        console_attach = cli_props.containsKey(UiOption.AttachConsole.pname());
        if (console_attach) {
            console_listener = new ConsoleListener(this, consoleCb);
            value = console_listener.getConsoleHostAddress();
            if (myClassName.equals(DuccManagedReservationSubmit.class.getName())) {
              value += "?splitstreams";   // Add a query string so APs have separate streams
            }
        } else if (suppress_console_log) {
            value = "suppress";
        } else {
            return;
        }
        // Set the console "suppress" flag or the host:port for the console listener into the env
        String key = UiOption.Environment.pname();
        String env = cli_props.getProperty(key);
        if (env == null) {
            env = "DUCC_CONSOLE_LISTENER=" + value;
        } else {
            env += " DUCC_CONSOLE_LISTENER=" + value;
        }
        cli_props.setProperty(key, env);
    }

    /*
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

}
