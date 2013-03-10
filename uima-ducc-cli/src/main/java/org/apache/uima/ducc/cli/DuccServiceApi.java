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
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.uima.ducc.common.IDucc;
import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.dispatcher.DuccEventHttpDispatcher;
import org.apache.uima.ducc.transport.event.ServiceModifyEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryReplyEvent;
import org.apache.uima.ducc.transport.event.ServiceRegisterEvent;
import org.apache.uima.ducc.transport.event.ServiceReplyEvent;
import org.apache.uima.ducc.transport.event.ServiceStartEvent;
import org.apache.uima.ducc.transport.event.ServiceStopEvent;
import org.apache.uima.ducc.transport.event.ServiceUnregisterEvent;
import org.apache.uima.ducc.transport.event.sm.IService;


/**
 * DUCC service API
 */

public class DuccServiceApi 
    implements IService,
               IServiceApi
{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String ducc_home = null;
     
    DuccProperties ducc_properties = null;
    Properties     jvmargs = null;     // -D vars from jvm args, as properties

    //String broker = null;
    //String jms_provider = "activemq";
    //String endpoint_name = "ducc.sm.request.endpoint";
    //String endpoint_type = "ducc.sm.request.endpoint.type";    
    DuccEventHttpDispatcher dispatcher;
    
    String sm_port = "ducc.sm.http.port";
    String sm_host = "ducc.sm.http.node";
    String endpoint = null;

    boolean debug = false;

    static boolean init_done = false;

    public DuccServiceApi()
    {
    }

    static void usage(String msg)
    {
        if ( msg != null ) {
            System.out.println(msg);
        }
        System.out.println("Usage:");
        System.exit(1);
    }

	static void usage(Options options) 
    {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(DuccUiConstants.help_width);
		formatter.printHelp(DuccServiceApi.class.getName(), options);
        System.exit(1);
	}

	@SuppressWarnings("static-access")
	private void addOptions(Options options) 
    {
        //
        // Verbs here
        //
		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceVerb.Register.decode())
                          .withDescription(ServiceVerb.Register.description())
                          .withArgName    (ServiceVerb.Register.argname())
                          .hasOptionalArg ()
                          .create         ()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceVerb.Unregister.decode())
                          .withDescription(ServiceVerb.Unregister.description())
                          .withArgName    (ServiceVerb.Unregister.argname())
                          .hasArg         (true)
                          .create         ()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceVerb.Start.decode())
                          .withDescription(ServiceVerb.Start.description())
                          .withArgName    (ServiceVerb.Start.argname())
                          .hasArg         (true)
                          .create         ()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceVerb.Stop.decode())
                          .withDescription(ServiceVerb.Stop.description())
                          .withArgName    (ServiceVerb.Stop.argname())
                          .hasArg         (true)
                          .create         ()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceVerb.Modify.decode())
                          .withDescription(ServiceVerb.Modify.description())
                          .withArgName    (ServiceVerb.Modify.argname())
                          .hasArg         (true)
                          .create         ()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceVerb.Query.decode())
                          .withDescription(ServiceVerb.Query.description())
                          .hasOptionalArg()
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceVerb.Debug.decode())
                          .withDescription(ServiceVerb.Debug.description())
                          .hasArg         (false)
                          .create         ()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceVerb.Help.decode())
                          .withDescription(ServiceVerb.Help.description())
                          .hasArg         (false)
                          .create         ()
                          );

        //
        // Options here
        //
		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceOptions.Autostart.decode())
                          .withDescription(ServiceOptions.Autostart.description())
                          .withArgName    (ServiceOptions.Autostart.argname())
                          .withDescription("Change autostart setting for registered service.")
                          .hasArg(true)
                          .create()
                          );


		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceOptions.Instances.decode())
                          .withDescription(ServiceOptions.Instances.description())
                          .withArgName    (ServiceOptions.Instances.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceOptions.Activate.decode())
                          .withDescription(ServiceOptions.Activate.description())
                          .withArgName    (ServiceOptions.Activate.argname())
                          .hasArg(false)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (ServiceOptions.Update.decode())
                          .withDescription(ServiceOptions.Update.description())
                          .withArgName    (ServiceOptions.Update.argname())
                          .hasArg(false)
                          .create()
                          );


        //
        // Services options that more correctly belong in properties file here
        //
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
        // Other directives are not supported for registered services.

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ClasspathOrder.decode()) 
                          .withDescription(RegistrationOption.ClasspathOrder.description()) 
                          .withArgName    (RegistrationOption.ClasspathOrder.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.Description.decode()) 
                          .withDescription(RegistrationOption.Description.description()) 
                          .withArgName    (RegistrationOption.Description.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ProcessDD.decode()) 
                          .withDescription(RegistrationOption.ProcessDD.description()) 
                          .withArgName    (RegistrationOption.ProcessDD.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ProcessMemorySize.decode()) 
                          .withDescription(RegistrationOption.ProcessMemorySize.description()) 
                          .withArgName    (RegistrationOption.ProcessMemorySize.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ProcessClasspath.decode()) 
                          .withDescription(RegistrationOption.ProcessClasspath.description()) 
                          .withArgName    (RegistrationOption.ProcessClasspath.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ProcessJvmArgs.decode()) 
                          .withDescription(RegistrationOption.ProcessJvmArgs.description()) 
                          .withArgName    (RegistrationOption.ProcessJvmArgs.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ProcessEnvironment.decode()) 
                          .withDescription(RegistrationOption.ProcessEnvironment.description()) 
                          .withArgName    (RegistrationOption.ProcessEnvironment.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ProcessFailuresLimit.decode()) 
                          .withDescription(RegistrationOption.ProcessFailuresLimit.description()) 
                          .withArgName    (RegistrationOption.ProcessFailuresLimit.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.SchedulingClass.decode()) 
                          .withDescription(RegistrationOption.SchedulingClass.description()) 
                          .withArgName    (RegistrationOption.SchedulingClass.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.WorkingDirectory.decode()) 
                          .withDescription(RegistrationOption.WorkingDirectory.description()) 
                          .withArgName    (RegistrationOption.WorkingDirectory.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.LogDirectory.decode()) 
                          .withDescription(RegistrationOption.LogDirectory.description()) 
                          .withArgName    (RegistrationOption.LogDirectory.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.Jvm.decode()) 
                          .withDescription(RegistrationOption.Jvm.description()) 
                          .withArgName    (RegistrationOption.Jvm.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ServiceDependency.decode()) 
                          .withDescription(RegistrationOption.ServiceDependency.description()) 
                          .withArgName    (RegistrationOption.ServiceDependency.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ServiceLinger.decode()) 
                          .withDescription(RegistrationOption.ServiceLinger.description()) 
                          .withArgName    (RegistrationOption.ServiceLinger.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ServiceRequestEndpoint.decode()) 
                          .withDescription(RegistrationOption.ServiceRequestEndpoint.description()) 
                          .withArgName    (RegistrationOption.ServiceRequestEndpoint.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ServicePingClass.decode()) 
                          .withDescription(RegistrationOption.ServicePingClass.description()) 
                          .withArgName    (RegistrationOption.ServicePingClass.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ServicePingClasspath.decode()) 
                          .withDescription(RegistrationOption.ServicePingClasspath.description()) 
                          .withArgName    (RegistrationOption.ServicePingClasspath.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ServicePingJvmArgs.decode()) 
                          .withDescription(RegistrationOption.ServicePingJvmArgs.description()) 
                          .withArgName    (RegistrationOption.ServicePingJvmArgs.argname())
                          .hasArg(true)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ServicePingDoLog.decode()) 
                          .withDescription(RegistrationOption.ServicePingDoLog.description()) 
                          .withArgName    (RegistrationOption.ServicePingDoLog.argname())
                          .hasArg(false)
                          .create()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ServicePingTimeout.decode()) 
                          .withDescription(RegistrationOption.ServicePingTimeout.description()) 
                          .withArgName    (RegistrationOption.ServicePingTimeout.argname())
                          .hasArg(true)
                          .create()
                          );
    }

    synchronized protected void init()
    {

        if ( init_done ) return;

        ducc_home = Utils.findDuccHome();
        if ( ducc_home == null ) {
            usage("DUCC_HOME must be set.");
        }

        String propsfile = ducc_home + "/resources/ducc.properties";
        try {
            ducc_properties = new DuccProperties();
            ducc_properties.load(propsfile);

            String host = ducc_properties.getStringProperty(sm_host);
            String port = ducc_properties.getStringProperty(sm_port);

            if ( host == null ) {
                throw new IllegalStateException(sm_host + " is not set in ducc.properties");
            }
        
            if ( port == null ) {
                throw new IllegalStateException(sm_port + " is not set in ducc.properties");
            }
            
            String targetUrl = "http://"+ host + ":" + port + "/sm";
            dispatcher = new DuccEventHttpDispatcher(targetUrl);

//             String en = ducc_properties.getStringProperty(endpoint_name);
//             String et = ducc_properties.getStringProperty(endpoint_type);
//             endpoint = jms_provider
//                 + ":" 
//                 + et 
//                 + ":"
//                 + en;

//             broker = ducc_properties.get("ducc.broker.protocol") 
//                 + "://"
//                 + ducc_properties.get("ducc.broker.hostname")
//                 + ":"
//                 + ducc_properties.get("ducc.broker.port");
//             String decoration = ducc_properties.getStringProperty("ducc.broker.url.decoration");
//             if ( decoration != null ) {
//                 broker = broker
//                     + "?"
//                     + decoration;
//             }
        } catch ( Throwable t ) {
            usage("Error loading configuration: " + t.getMessage());
        }

//        if ( debug ) {
//            System.out.println("DUCC_HOME   :  " + ducc_home);
//            System.out.println("SM Endpoint :  " + endpoint);
//            System.out.println("Broker      :  " + broker);            
//        }

        init_done = true;
    }

    private Pair<Integer, String> getId(CommandLine cl, ServiceVerb verb)
    {
        String sid = cl.getOptionValue(verb.decode());
        if ( sid == null ) {
            throw new IllegalArgumentException("Missing id for --" + verb.decode() + ".");
        }
        
        int id = -1;
        try {
            id = Integer.parseInt(sid);
            return new Pair<Integer, String>(id, null);
        } catch ( NumberFormatException e ) {
            // nothing
        }
        if ( sid.startsWith(ServiceType.UimaAs.decode()) || sid.startsWith(ServiceType.Custom.decode()) ) {
            return new Pair<Integer, String>(-1, sid);
        }
        throw new IllegalArgumentException("Invalid id; must be numeric or start with " + ServiceType.UimaAs.decode() + " or " + ServiceType.Custom.decode() + ".");
    }

    private int getInstances(CommandLine cl, int dflt)
    {
        String nstncs = cl.getOptionValue(ServiceOptions.Instances.decode());
        if ( nstncs == null ) {
            return dflt;
        }

        int instances = 0;
        try {
            instances = Integer.parseInt(nstncs);
        } catch ( NumberFormatException e ) {
            System.out.println (ServiceOptions.Instances.decode() + " " + nstncs + " is not numeric.");
            doExit(1);
        }
        if ( instances <= 0 ) {
            System.out.println(ServiceOptions.Instances.decode() + " " + nstncs + " must be > 0");
            doExit(1);
        }

        return instances;
    }

    private Trinary getAutostart(CommandLine cl)
    {
        String auto = cl.getOptionValue(ServiceOptions.Autostart.decode(), null);
        if ( auto == null ) {
            return Trinary.Unset;
        }

        Trinary answer = Trinary.encode(auto);

        if ( answer == Trinary.Unset ) {
            System.out.println("--" + ServiceOptions.Autostart.decode()  + " " + auto + " is not 'true' or 'false'");
            doExit(1);
        }

        return answer;
    }

    private boolean getActivate(CommandLine cl)
    {
        return cl.hasOption(ServiceOptions.Activate.decode());
    }

    private boolean getUpdate(CommandLine cl)
    {
        return cl.hasOption(ServiceOptions.Update.decode());
    }

    private void overrideProperties(CommandLine cl, DuccProperties props, RegistrationOption opt)
    {

        String k = opt.decode();
        String v = cl.getOptionValue(k, null);
        if ( v == null ) return;
        props.put(k, v);
    }

    private String getLinger(DuccProperties props)
    {
        String kw = RegistrationOption.ServiceLinger.decode();
        String default_linger = ducc_properties.getStringProperty("ducc.sm.default.linger", "5000");
        String linger = props.getStringProperty(kw, default_linger);
        long actual = 0;
        try {             
            actual = Long.parseLong(linger); // make sure it's an int
        } catch ( NumberFormatException e ) {
            throw new IllegalArgumentException(kw + " is not numeric: " + linger);
        }
        return Long.toString(actual);
    }

    synchronized protected DuccProperties getPropsFile(CommandLine cl)
        throws Throwable
    {
        DuccProperties reply = new DuccProperties();

        //
        // First read the properties file if given in
        //    ducc_services --register propsfile
        String props = cl.getOptionValue(ServiceVerb.Register.decode());
        if ( props != null ) {
            reply.load(new FileInputStream(props));
            if ( debug ) {
                System.out.println("Service specification file:");
                for ( Object key: reply.keySet() ) {
                    System.out.println("    Key: " + key + "  Value: " + reply.getStringProperty((String)key));
                }
            }            
        }

        // 
        // Now pull in the override props.
        //
        overrideProperties(cl, reply, RegistrationOption.ClasspathOrder);
        overrideProperties(cl, reply, RegistrationOption.Description);
        overrideProperties(cl, reply, RegistrationOption.ProcessDD);
        overrideProperties(cl, reply, RegistrationOption.ProcessMemorySize);
        overrideProperties(cl, reply, RegistrationOption.ProcessClasspath);
        overrideProperties(cl, reply, RegistrationOption.ProcessJvmArgs);
        overrideProperties(cl, reply, RegistrationOption.ProcessEnvironment);
        overrideProperties(cl, reply, RegistrationOption.ProcessFailuresLimit);
        overrideProperties(cl, reply, RegistrationOption.SchedulingClass);
        overrideProperties(cl, reply, RegistrationOption.WorkingDirectory);
        overrideProperties(cl, reply, RegistrationOption.LogDirectory);
        overrideProperties(cl, reply, RegistrationOption.Jvm);
        overrideProperties(cl, reply, RegistrationOption.ServiceDependency);
        overrideProperties(cl, reply, RegistrationOption.ServiceLinger);
        overrideProperties(cl, reply, RegistrationOption.ServicePingClass);
        overrideProperties(cl, reply, RegistrationOption.ServiceRequestEndpoint);
        overrideProperties(cl, reply, RegistrationOption.ServicePingClasspath);
        overrideProperties(cl, reply, RegistrationOption.ServicePingJvmArgs);
        overrideProperties(cl, reply, RegistrationOption.ServicePingDoLog);
        overrideProperties(cl, reply, RegistrationOption.ServicePingTimeout);

        // now bop through the properties and make sure they and their values all valid
        for ( Object o : reply.keySet() ) {
            String k = (String) o;
            
            RegistrationOption opt = RegistrationOption.encode(k);
            if ( opt == RegistrationOption.Unknown ) {
                throw new IllegalArgumentException("Invalid regisration option: " + k);
            }            

            switch ( opt ) {
                case ClasspathOrder:
                    String v = reply.getStringProperty(RegistrationOption.ClasspathOrder.decode());
                    if ( v == null ) continue;
                    if ( ClasspathOrderParms.encode(v) == ClasspathOrderParms.Unknown) {
                        throw new IllegalStateException("Invalid value for " + RegistrationOption.ClasspathOrder.decode());
                    }           
                    break;
            }
        }

        //
        // Now: let's resolve placeholders.
        //
        String jvmarg_string = reply.getProperty(RegistrationOption.ProcessJvmArgs.decode());
        jvmargs = DuccUiUtilities.jvmArgsToProperties(jvmarg_string);
        DuccUiUtilities.resolvePropertiesPlaceholders(reply, jvmargs);
        return reply;
    }


    /**
     * @param endpoint This is 'my' service endpoint
     * @param props    This is the service properties file with the dependencies in it.
     */
    void resolve_service_dependencies(String endpoint, DuccProperties props)
    {
        String deps = props.getProperty(RegistrationOption.ServiceDependency.decode());
        deps = DuccUiUtilities.resolve_service_dependencies(endpoint, deps, jvmargs);
        if ( deps != null ) {
            props.setProperty(RegistrationOption.ServiceDependency.decode(), deps);
        }
    }

    String extractEndpoint(DuccProperties service_props, String working_dir)
    {
        // If claspath is not specified, pick it up from the submitter's environment
        String classpath = service_props.getStringProperty(RegistrationOption.ProcessClasspath.decode(), System.getProperty("java.class.path"));
        service_props.setProperty(RegistrationOption.ProcessClasspath.decode(), classpath);
        
        // No endpoint, resolve from the DD.
        String dd = service_props.getStringProperty(RegistrationOption.ProcessDD.decode()); // will throw if can't find the prop
        endpoint = DuccUiUtilities.getEndpoint(working_dir, dd, jvmargs);
        if ( debug ) {
            System.out.println("Service endpoint resolves to " + endpoint);
        }
        return endpoint;
    }

    /**
     * @param props Name of file in standard Java properies format with the service specification.
     * @return 
     */
    public ServiceReplyEvent register(DuccProperties service_props, int instances, Trinary autostart)
        throws Exception
    {
        //
        // TODO: Crypto?
        //

        String k_wd = RegistrationOption.WorkingDirectory.decode();
        String working_dir = service_props.getStringProperty(k_wd, System.getProperty("user.dir"));
        service_props.setProperty(k_wd, working_dir);

        // Employ default log directory if not specified
        String k_ld = RegistrationOption.LogDirectory.decode();
		String log_directory = service_props.getStringProperty(k_ld, System.getProperty("user.home") + IDucc.userLogsSubDirectory);
        if( ! log_directory.startsWith(File.separator)) {
            // relative log directory was specified - default to user's home + relative directory
            // TODO: This logic comes from submit.  But is it right?  Log dir relative to $HOME instread of job's working dir?
            if (log_directory.endsWith(File.separator)) {
                log_directory = System.getProperty("user.home") + log_directory;
            } else {
                log_directory = System.getProperty("user.home") + File.separator + log_directory;
            }
		}

        // Insure a linger time is set
        String k_lin = RegistrationOption.ServiceLinger.decode();
        String linger = getLinger(service_props);
        service_props.setProperty(k_lin, linger);
        
		// tack on "services" to complete logging directory
        // TODO: Again, from service submit - is it really correct to force "/services" into the path if the
        //       user specifies the path?
		// if(log_directory.endsWith(File.separator)) {
// 			log_directory = log_directory+"services";
// 		}
// 		else {
// 			log_directory = log_directory+File.separator+"services";
// 		}
		service_props.setProperty(k_ld, log_directory);

        //
        // Establish my endpoint
        //
        String  endpoint = service_props.getStringProperty(RegistrationOption.ServiceRequestEndpoint.decode(), null);
        if ( endpoint == null ) {               // not custom ... must be uima-as (or fail)
            endpoint = extractEndpoint(service_props, working_dir);
        } else if ( endpoint.startsWith(ServiceType.Custom.decode()) ) {

            // must have a pinger specified
            if ( service_props.getProperty(RegistrationOption.ServicePingClass.decode()) == null ) {
                throw new IllegalArgumentException("Custom service is missing ping class name.");
            }
 
            String k_scp = RegistrationOption.ServicePingClasspath.decode();
            String classpath = service_props.getStringProperty(k_scp, System.getProperty("java.class.path"));            
            service_props.setProperty(k_scp, classpath);
        } else if ( endpoint.startsWith(ServiceType.UimaAs.decode()) ) {
            // Infer the classpath
            String classpath = service_props.getStringProperty(RegistrationOption.ProcessClasspath.decode(), System.getProperty("java.class.path"));
            service_props.setProperty(RegistrationOption.ProcessClasspath.decode(), classpath);

            // Given ep must match inferred ep. Use case: an application is wrapping DuccServiceApi and has to construct
            // the endpoint as well.  The app passes it in and we insure that the constructed endpoint matches the one
            // we extract from the DD - the job will fail otherwise, so we catch this early.
            String verify_endpoint = extractEndpoint(service_props, working_dir);            
            if ( !verify_endpoint.equals(endpoint) ) {
                throw new IllegalArgumentException("Endpoint from --service_request_endpoint does not match endpoint ectracted from UIMA DD" 
                                                   + "\n--service_request_endpoint: " + endpoint 
                                                   + "\nextracted:                : " + verify_endpoint );
            }
        } else {
            throw new IllegalArgumentException("Invalid custom endpoint: " + endpoint);
        }

        // work out stuff I'm dependendent upon
        resolve_service_dependencies(endpoint, service_props);

        //
        // Finally set up the the event object and ship it!
        //
        // DuccEventDispatcher dispatcher = connect();

        ServiceRegisterEvent ev = new ServiceRegisterEvent(DuccUiUtilities.getUser(), instances, autostart, endpoint, service_props);
        ServiceReplyEvent reply = null;

        try {
            reply = (ServiceReplyEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            // TODO: print a nice error.  For now, we spew the stack.
            e.printStackTrace();
        } finally {
            dispatcher.close();
        }

//         try {
//         	reply = (ServiceReplyEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
//         }
//         finally {
//         	context.stop();
//         }

        return reply;
	}

    /**
     * @param id The full service id as returned by register
     * @return 
     */
    public ServiceReplyEvent unregister(int intId, String strId)
        throws Exception
    {
        ServiceUnregisterEvent ev = new ServiceUnregisterEvent(DuccUiUtilities.getUser(), intId, strId);
        //DuccEventDispatcher dispatcher = connect();
        ServiceReplyEvent reply = null;
        
        try {
            reply = (ServiceReplyEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dispatcher.close();
        }

//         try {
//         	reply = (ServiceReplyEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
//         }
//         finally {
//         	context.stop();
//         }

        return reply;
	}

    /**
     * @param props Name of file in standard Java properies format with the service specification.
     * @return 
     */
    public ServiceReplyEvent start(int intId, String strId, int instances, boolean update)
        throws Exception
    {

        ServiceStartEvent ev = new ServiceStartEvent(DuccUiUtilities.getUser(), intId, strId);
        ev.setInstances(instances);
        ev.setUpdate(update);
        
        ServiceReplyEvent reply = null;
        try {
            reply = (ServiceReplyEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dispatcher.close();
        }

        return reply;
    }


    /**
     * @param props Name of file in standard Java properies format with the service specification.
     * @return 
     */
    public ServiceReplyEvent stop(int intId, String strId, int instances, boolean update)
        throws Exception
    {

        ServiceStopEvent ev = new ServiceStopEvent(DuccUiUtilities.getUser(), intId, strId);
        ev.setInstances(instances);
        ev.setUpdate(update);
        
        ServiceReplyEvent reply = null;
        try {
            reply = (ServiceReplyEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dispatcher.close();
        }

        return reply;
    }

    /**
     * @param id The id of a registered service.
     * @param instances The nubmer of instances of the service to start.
     * @param autostart Update to autostart status of the registered service.
     * @return 
     */
    public ServiceReplyEvent modify(int intId, String strId, int instances, Trinary autostart, boolean activate)
        throws Exception
    {

        ServiceModifyEvent ev = new ServiceModifyEvent(DuccUiUtilities.getUser(), intId, strId);
        ev.setInstances(instances);
        ev.setAutostart(autostart);
        ev.setActivate(activate);
        
        ServiceReplyEvent reply = null;
        try {
            reply = (ServiceReplyEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dispatcher.close();
        }

        return reply;
    }

    /**
     * @param props Name of file in standard Java properies format with the service specification.
     * @return 
     */
    public ServiceQueryReplyEvent query(int intId, String strId)
        throws Exception
    {
        ServiceQueryEvent ev = new ServiceQueryEvent(DuccUiUtilities.getUser(), intId, strId);
        ServiceQueryReplyEvent reply = null;

        try {
            reply = (ServiceQueryReplyEvent) dispatcher.dispatchAndWaitForDuccReply(ev);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dispatcher.close();
        }

        return reply;
    }

    synchronized ServiceVerb extract_verb(CommandLine cl)
    {
        boolean found_verb = false;
        ServiceVerb verb = ServiceVerb.Unknown;
        
        for ( ServiceVerb v : ServiceVerb.values() ) {
        	if ( cl.hasOption(v.decode()) ) {
                if ( found_verb ) {
                    usage("Duplicate option " + v + ": not allowd.");
                } else {
                    found_verb = true;
                    verb = v;
                }
            }
        }

        return verb;
    }

    public void print_reply(ServiceVerb verb, ServiceReplyEvent ev)
    {
        String result = (ev.getReturnCode() == ServiceCode.OK) ? "succeeded" : "failed";
        String reason = (ev.getReturnCode() == ServiceCode.OK) ? "" : ": " +ev.getMessage();
        String action = "Service " + verb.decode();
        String msg = (action + " " + result + " ID " + ((ev.getId() == null) ? "<none>" : ev.getId().toString()) + " endpoint " + ev.getEndpoint() + reason);
        switch ( verb ) {
           case Register:
           case Unregister:
           case Start:
           case Stop:
           case Modify:
               System.out.println(msg);
               break;
           case Query:
               System.out.println(ev.toString());
               break;
        }

        if ( ev.getReturnCode() != ServiceCode.OK ) {
            throw new IllegalStateException("Return code NOTOK in rervice reply.");
        }
    }

    public void run(String[] args)
    	throws Throwable
    {
        Options options = new Options();
        synchronized(this) {
            addOptions(options);
        }

        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = null;
		try {
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
            usage("Cannot parse command line: " + e.getMessage());            
		}

        /*
         * give help & exit when requested
         */
        if (commandLine.hasOption(ServiceVerb.Help.decode())) {
        	usage(options);
        }

        if(commandLine.getOptions().length == 0) {
            usage(options);
        }

        if (commandLine.hasOption(ServiceVerb.Debug.decode())) {
            debug = true;
        }
        
        DuccProperties props = null;
        int instances = 0;
        boolean activate = false;
        boolean update = false;
        Pair<Integer, String> id = null;
        Trinary autostart;
        try { 
            ServiceVerb verb = extract_verb(commandLine);
            ServiceReplyEvent reply = null;
            switch ( verb ) {
                case Register:                    
                    props = getPropsFile(commandLine);
                    instances = getInstances(commandLine, 1);
                    autostart = getAutostart(commandLine);
                    reply = register(props, instances, autostart);                    
                    break;
                case Unregister:
                    id = getId(commandLine, verb);
                    reply = unregister(id.first(), id.second());
                    break;
                case Start:
                    id = getId(commandLine, verb);
                    instances = getInstances(commandLine, -1);
                    update = getUpdate(commandLine);
                    reply = start(id.first(), id.second(), instances, update);                    
                    break;
                case Stop:
                    id = getId(commandLine, verb);
                    instances = getInstances(commandLine, -1);
                    update = getUpdate(commandLine);
                    reply = stop(id.first(), id.second(), instances, update);
                    break;
                case Modify:
                    id = getId(commandLine, verb);
                    instances = getInstances(commandLine, -1);
                    autostart = getAutostart(commandLine);
                    activate  = getActivate(commandLine);
                    reply = modify(id.first(), id.second(), instances, autostart, activate);
                    break;
                case Query:
                    if (commandLine.getOptionValue(verb.decode()) == null ) {
                        reply = query(-1, null);
                    } else {
                        id = getId(commandLine, verb);
                        reply = query(id.first(), id.second());
                    }
                    break;
                case Unknown:
                    throw new IllegalArgumentException("Missing Service verb (--register --unregister --start --stop --query --modify)");
            }
            print_reply(verb, reply);
        } catch ( Throwable t ) {
            System.out.println("Service command fails: " + t.getMessage());
            doExit(1);
        }
    }

    //
    // single common exit point for CL invoction
    //
    protected void doExit(int rc)
    {
        System.exit(rc);
    }

    /**
     * DuccServiceApi <options>
     * Where options:
     *    -r --register <properties>
     *    -u --unregister <id>
     *    -q --query <id>
     *    --start <id>
     *    --stop <id>
     *    -m --modify <properties>
     *    -f --fetch <id>
     *
     *  Properties file:
     *  type = UIMA-AS | Custom
     *  endpoint = <amq endoint>      # if UIMA-AS
     *  broker   = <broker url>       # if UIMA-AS
     *  user     = userid
     *
     *  Service is identified as:
     *  type:endpoint:broker
     *  e.g.  UIMA-AS@FixedSleepAE@tcp://bluej02
     */
	public static void main(String[] args) 
    {

		try {
			DuccServiceApi api = new DuccServiceApi();
            api.init();
			api.run(args);
            api.doExit(0);
		} catch (Throwable e) {
			e.printStackTrace();           // should not get this, hopefully have trapped everything
            System.exit(1);
		}
	}
	
}
