
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
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.uima.ducc.common.IDucc;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.dispatcher.DuccEventHttpDispatcher;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;

public abstract class CliBase
    extends DuccUi 
{

    private boolean init_done = false;
    protected String ducc_home;
    protected DuccProperties ducc_properties;
    protected DuccEventHttpDispatcher dispatcher;

    protected Options options;
    protected CommandLineParser parser;
    protected CommandLine commandLine;
    
    protected long friendlyId = -1;

    protected Properties cli_props;
    protected ArrayList<String> errors   = new ArrayList<String>();
    protected ArrayList<String> warnings = new ArrayList<String>();
    protected ArrayList<String> messages = new ArrayList<String>();
    
    abstract void addOptions(Options options);
    abstract boolean  execute() throws Exception;

    protected ConsoleListener  console_listener = null;


    String getLogDirectory(DuccProperties properties)
    {
        /*
         * employ default log directory if not specified
         */
        String log_directory = properties.getProperty("log_directory");
        if(log_directory == null) {
            // no log directory was specified - default to user's home + "/ducc/logs"
            log_directory = System.getProperty("user.home")+IDucc.userLogsSubDirectory;
        } else {
            if(log_directory.startsWith(File.separator)) {
                // absolute log directory was specified
            } else {
                // relative log directory was specified - default to user's home + relative directory
                if(log_directory.endsWith(File.separator)) {
                    log_directory = System.getProperty("user.home")+log_directory;
                }
                else {
                    log_directory = System.getProperty("user.home")+File.separator+log_directory;
                }
            }
        }
        properties.setProperty("log_directory", log_directory);

        /*
         * make sure the logdir is actually legal.
         */
        File f = new File(log_directory);
        if ( f.exists() ) {
            if ( !f.isDirectory() ) {
                addError("Specified log_directory is not a directory: " + log_directory);
                return null;
            }
        } else if ( !f.mkdirs() ) {            
            addError("Cannot create log_directory: " + log_directory);
            return null;
        }

        if ( ! f.canWrite() ) {
            addError("Log directory exists but cannot be written: " + f);
            return null;
        }
        return log_directory;
    }

    
    synchronized void init(String[] args, Properties cli_props, String host_s, String port_s, String servlet)
    	throws Exception
    {
        if ( init_done ) return;

        ducc_home = Utils.findDuccHome();

        this.cli_props = cli_props;
        options = new Options();
        parser = new PosixParser();
        addOptions(options);
        commandLine = parser.parse(options, args);

        if (commandLine.hasOption(DuccUiConstants.name_help)) {
            usage(null);
        }
        if(commandLine.getOptions().length == 0) {
            usage(null);
        }

        String propsfile = ducc_home + "/resources/ducc.properties";
        ducc_properties = new DuccProperties();
        ducc_properties.load(propsfile);

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

        init_done = true;
    }

    void saveSpec(String id, String name, DuccProperties props) 
        throws Exception
    {
        String directory = props.getProperty("log_directory") + File.separator + id;
        String fileName = directory + File.separator + name;
        File f = new File(directory);
        f.mkdirs();

        String comments = null;
        FileOutputStream fos = null;
        OutputStreamWriter out = null;
        fos = new FileOutputStream(fileName);
        out = new OutputStreamWriter(fos);

        String key = SpecificationProperties.key_signature;
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
                addWarning(key + " environment conflict: " + target + " takes precedence over " + source);
			} else {
				target += "="+environment_properties.getProperty(source);
				environment_string += " "+target;
				requestProps.setProperty(key, environment_string);
			}
		}
	}

    void usage(String message)
    {
        if ( message != null ) {
            System.out.println(message);
        }
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(DuccUiConstants.help_width);
        formatter.printHelp(DuccletSubmit.class.getName(), options);
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

    public boolean hasProperty(String key)
    {
        return cli_props.containsKey(key);
    }

    public String getProperty(String key)
    {
        return (String) cli_props.getProperty(key);
    }

    synchronized void addWarning(String w)
    {
        this.warnings.add(w);
    }


    synchronized void addError(String e )
    {
        this.errors.add(e);
    }


    synchronized void addMessage(String m)
    {
        this.messages.add(m);
    }


    synchronized void addErrors(ArrayList<String> e)
    {
        this.errors.addAll(e);
    }

    synchronized void addWarnings(ArrayList<String> w)
    {
        this.warnings.addAll(w);
    }

    synchronized public String[] getMessages()
    {
        return messages.toArray(new String[messages.size()]);
    }

    synchronized public String[] getWarnings()
    {
        return warnings.toArray(new String[warnings.size()]);
    }

    synchronized public String[] getErrors()
    {
        return errors.toArray(new String[errors.size()]);
    }

    synchronized long getDuccId()
    {
        return friendlyId;
    }

    protected void startConsoleListener()
    	throws Throwable
    {
    	console_listener = new ConsoleListener(this);
        Thread t = new Thread(console_listener);
        t.start();
    }

    protected void stopConsoleListener()
    {
        if ( console_listener != null ) {
            console_listener.shutdown();
            console_listener = null;
        }
    }

    protected void set_console_port(DuccProperties props, String key)
    {
        if ( key != null ) {         
            if ( console_listener == null ) {
                addWarning("Attempt to set console port but listener is not running.");
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

    public void waitForCompletion()
    {
        if ( console_listener != null ) {
            console_listener.waitForCompletion();
        }
        console_listener = null;
    }

    public void closeConsole()
    {
        if ( console_listener != null ) {
            console_listener.shutdown();
        }
        console_listener = null;
    }

}
