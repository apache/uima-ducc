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

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
import org.apache.uima.ducc.transport.event.ServiceDisableEvent;
import org.apache.uima.ducc.transport.event.ServiceEnableEvent;
import org.apache.uima.ducc.transport.event.ServiceIgnoreEvent;
import org.apache.uima.ducc.transport.event.ServiceModifyEvent;
import org.apache.uima.ducc.transport.event.ServiceObserveEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryEvent;
import org.apache.uima.ducc.transport.event.ServiceRegisterEvent;
import org.apache.uima.ducc.transport.event.ServiceStartEvent;
import org.apache.uima.ducc.transport.event.ServiceStopEvent;
import org.apache.uima.ducc.transport.event.ServiceUnregisterEvent;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceType;
import org.apache.uima.ducc.transport.event.sm.IService.Trinary;
import org.apache.uima.ducc.transport.event.sm.IServiceReply;


/**
 * Handle registered services. This class is also the implementation of the
 * DUCC service CLI which internally uses its API to implement itself.  Details on the
 * DUCC service CLI are found in the<a href="/doc/duccbook.html#DUCC_CLI_SERVICES">DUCC CLI reference.</a>
 */
public class DuccServiceApi 
    extends CliBase
{

    String endpoint = null;
    IDuccCallback callback = null;

    UiOption[] registration_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Description,
        UiOption.Administrators,
        UiOption.SchedulingClass,
        UiOption.LogDirectory,
        UiOption.WorkingDirectory,
        UiOption.Jvm,
        UiOption.ProcessJvmArgs,
        UiOption.Classpath,
        UiOption.Environment,
        UiOption.ProcessMemorySize,
        UiOption.ProcessDD,
        UiOption.ProcessExecutable,
        UiOption.ProcessExecutableArgs,
        UiOption.ProcessInitializationTimeMax,
        UiOption.ProcessDebug,

        UiOption.ClasspathOrder,
        // UiOption.Specification          // not used for registration
        UiOption.ServiceDependency,
        UiOption.ServiceRequestEndpoint,
        UiOption.ServiceLinger,

        UiOption.ServicePingArguments,
        UiOption.ServicePingClass,
        UiOption.ServicePingClasspath,
        UiOption.ServicePingJvmArgs,
        UiOption.ServicePingTimeout,
        UiOption.ServicePingDoLog,

        UiOption.InstanceFailureWindow,
        UiOption.InstanceFailureLimit,
        UiOption.InstanceInitFailureLimit,

        UiOption.Register,
        UiOption.Autostart,
        UiOption.Instances,
    }; 
   
    UiOption[] unregister_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Unregister,
        UiOption.RoleAdministrator,
    }; 


    /**
     * Service start arguments.
     */
    UiOption[] start_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Start,
        UiOption.Instances,
        UiOption.RoleAdministrator,
    }; 

    UiOption[] stop_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Stop,
        UiOption.Instances,
        UiOption.RoleAdministrator,
    }; 

    UiOption[] enable_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Enable,
        UiOption.RoleAdministrator,
    }; 

    UiOption[] disable_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Disable,
        UiOption.RoleAdministrator,
    }; 

    UiOption[] observe_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Observe,
        UiOption.RoleAdministrator,
    }; 

    UiOption[] ignore_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Ignore,
        UiOption.RoleAdministrator,
    }; 

    // This gets generated from the registratoin_options.
    UiOption[] modify_options;

    UiOption[] query_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Query,
    }; 

    // These options are only valid for CUSTOM services
    UiOption[] custom_only_options = {
            UiOption.ProcessExecutable,
            UiOption.ProcessExecutableArgs,
    }; 
       
    // These options are only valid for UIMA-AS services
    UiOption[] uimaas_only_options = {
            UiOption.ProcessDD,
            UiOption.Jvm,
            UiOption.ProcessJvmArgs,
            UiOption.ProcessDebug,
            UiOption.Classpath,
            UiOption.ClasspathOrder,
    }; 
    
    // These options are only valid for services with an explicit pinger
    UiOption[] pinger_only_options = {
            UiOption.ServicePingClasspath,
            UiOption.ServicePingJvmArgs,
            UiOption.ServicePingTimeout,
            UiOption.ServicePingDoLog,
    };

    public DuccServiceApi(IDuccCallback cb)
    {
        this.callback = cb;

        //
        // generate modify options, same as registration options, only with the verb
        // Modify insteady of Register, and on extra option, Activate.
        // The length here, based on registration options:
        //     minus 1 for ProcessDD
        //     minus 1 for ServiceRequestEndpoint
        //     plus 1 for RoleAdministrator
        //     ==> -1 length
        //
        modify_options = new UiOption[registration_options.length - 1];
        int i = 0;
        for ( UiOption o : registration_options ) {

            if ( o == UiOption.ProcessDD ) continue;           // disallowed for modify
            if ( o == UiOption.ServiceRequestEndpoint) continue;         // disallowed for modify

            if ( o == UiOption.Register ) o = UiOption.Modify;

            modify_options[i++] = o;
        }
        modify_options[i++] = UiOption.RoleAdministrator;
        // modify_options[i++] = UiOption.Activate;
    }

    private Pair<Integer, String> getId(UiOption opt)
    {

        String sid = cli_props.getProperty(opt.pname());

        if ( sid == null ) {
            throw new IllegalArgumentException("Missing service id: --id <id or endpoint>");
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

    private Trinary getAutostart()
    {
        String auto = cli_props.getStringProperty(UiOption.Autostart.pname(), null);
        if ( auto == null ) {
            return Trinary.Unset;
        }
        boolean val = Boolean.parseBoolean(auto);

        Trinary answer = Trinary.encode(val ? "true" : "false");

        if ( answer == Trinary.Unset ) {
            throw new IllegalArgumentException("--" + UiOption.Autostart.pname()  + " " + auto + " is not 'true' or 'false'");
        }

        return answer;
    }

    private int getInstances(int dflt)
    {
        String inst = cli_props.getProperty(UiOption.Instances.pname());
        if ( inst == null ) return dflt;

        int instances;
        try {
            instances = Integer.parseInt(inst);
        } catch ( NumberFormatException e ) {
            throw new IllegalArgumentException(UiOption.Instances.pname() + " " + inst + " is not numeric.");
        }

        if ( instances <= 0 ) {
            throw new IllegalArgumentException(UiOption.Instances.pname() + " " + inst + " must be > 0");
        }

        return instances;
    }


//    private boolean getActivate()
//    {
//        return cli_props.containsKey(UiOption.Activate.pname());
//    }

//    private boolean getUpdate()
//    {
//        return cli_props.containsKey(UiOption.Update.pname());
//    }

    private void setLinger()
    {
        String default_linger = DuccPropertiesResolver.get("ducc.sm.default.linger", "5000");
        String linger         = cli_props.getStringProperty(UiOption.ServiceLinger.pname(), default_linger);
        try {             
			Long.parseLong(linger); // make sure it's a long, don't care about the value
        } catch ( NumberFormatException e ) {
            throw new IllegalArgumentException(UiOption.ServiceLinger.pname() + " is not numeric: " + linger);
        }
    }

    /**
     * Attempt a fast-fail if a bad debug port is specified.  Fill in the host if not supplied.
     */
    private void enrichPropertiesForDebug(UiOption verb)
    {

        String debug_port = cli_props.getProperty(UiOption.ProcessDebug.pname());
        String debug_host = host_address;
        if ( debug_port == null )       return; 

        if ( debug_port.equals("off") ) {
            switch (verb ) {
                case Register:
                    System.out.println("Note: 'process_debug = off' removed; 'off' is valid only for --modify");
                    cli_props.remove(UiOption.ProcessDebug.pname());     // 'off' invalid for registration
                    return;
                case Modify:
                    return;
            }
        }

        if ( debug_port.contains(":") ) {
            String[] parts = debug_port.split(":");
            if ( parts.length != 2 ) {
                throw new IllegalArgumentException("Error: " + 
                                                   UiOption.ProcessDebug.pname() + 
                                                   " process_debug must be a single numeric port, or else of the form 'host:port'");
            }

            debug_host = parts[0];
            debug_port = parts[1];
        }

        try {
			Integer.parseInt(debug_port);
        } catch ( NumberFormatException e ) {
            throw new IllegalArgumentException("Invalid debug port specified, not numeric: " + debug_port);
        }

        cli_props.setProperty(UiOption.ProcessDebug.pname(), debug_host + ":" + debug_port);
    }

    private String extractEndpoint(String jvmargs)
    {
        String dd = cli_props.getStringProperty(UiOption.ProcessDD.pname()); // will throw if can't find the prop
        String working_dir = cli_props.getStringProperty(UiOption.WorkingDirectory.pname());
        endpoint = DuccUiUtilities.getEndpoint(working_dir, dd, jvmargs);
        if ( debug ) {
            System.out.println("DD service endpoint resolves to " + endpoint);
        }
        return endpoint;
    }

    private void discardOptions(UiOption[] ignored_options, String type) {
        for (UiOption opt : ignored_options) {
            if (cli_props.containsKey(opt.pname())) {
                message("WARNING - Option --" + opt.pname() + " is ignored for " + type + " services");
                cli_props.remove(opt.pname());
            }
        }
    }

    
    
    public UiOption[] getModifyOptions()
    {
        return modify_options;
    }

    /**
     * The register API is used to register a service with the service manager.
     *
     * @param args String rray of arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SERVICES">DUCC CLI reference.</a>
     * @return {@link IServiceReply IServiceReply} object with register status.
     */
    public IServiceReply register(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();
        init (this.getClass().getName(), registration_options, args, null, dp, callback, "sm");

        // Note: dp & cli_props are identical ... use only the class variable here for consistency
        
        setLinger();

        // Determine service type based on the endpoint (default is UIMA-AS)
        // For each type check for required options; also warn and drop any inappropriate options
        
        String  endpoint = cli_props.getStringProperty(UiOption.ServiceRequestEndpoint.pname(), null);
        if ( endpoint == null || endpoint.startsWith(ServiceType.UimaAs.decode()) ) {

            String uimaDD = cli_props.getStringProperty(UiOption.ProcessDD.pname(), null);
            if (uimaDD == null) {
                throw new IllegalArgumentException("Option --" + UiOption.ProcessDD.pname() + " is required for UIMA-AS services");
            }
            discardOptions(custom_only_options, ServiceType.UimaAs.decode());
            if ( ! cli_props.containsKey(UiOption.ServicePingClass.pname()) ) {
                discardOptions(pinger_only_options, "pinger-less UIMA-AS");
            }
            
            // Set default classpath if not specified - only used for UIMA-AS services
            String key_cp = UiOption.Classpath.pname();
            if (!cli_props.containsKey(key_cp)) {
                cli_props.setProperty(key_cp, System.getProperty("java.class.path"));
            }

            // Given ep must match inferred ep. Use case: an application is wrapping DuccServiceApi and has to construct
            // the endpoint as well.  The app passes it in and we insure that the constructed endpoint matches the one
            // we extract from the DD - the job will fail otherwise, so we catch this early.
            String jvmarg_string = cli_props.getProperty(UiOption.ProcessJvmArgs.pname());
            String inferred_endpoint = extractEndpoint(jvmarg_string);
            if (endpoint == null) {
                endpoint = inferred_endpoint;
            } else {
                // Check & strip any broker URL decorations on the endpoint
                endpoint = DuccUiUtilities.check_service_dependencies(null, endpoint);
                cli_props.setProperty(UiOption.ServiceRequestEndpoint.pname(), endpoint);    // SM uses both endpoint definitions !!!
                if ( !inferred_endpoint.equals(endpoint) ) {
                    throw new IllegalArgumentException("Specified endpoint does not match endpoint extracted from UIMA DD" 
                                                     + "\n --service_request_endpoint: " + endpoint 
                                                     + "\n                  extracted: " + inferred_endpoint );
                }
            }

            enrichPropertiesForDebug(UiOption.Register);
            
        } else if (endpoint.startsWith(ServiceType.Custom.decode())) {

            // Custom services must have a pinger, but the process_executable (& args) 
            // options may be omitted for a ping-only service.
            // When omitted other options such as autostart are irrelevant.
            if ( ! cli_props.containsKey(UiOption.ServicePingClass.pname()) ) {
                throw new IllegalArgumentException("Option --service_ping_class is required for CUSTOM services");
            }
            discardOptions(uimaas_only_options, ServiceType.Custom.decode());
            
            String key_cp = UiOption.ServicePingClasspath.pname();
            if (!cli_props.containsKey(key_cp)) {
                cli_props.setProperty(key_cp, System.getProperty("java.class.path"));
            }
        
        } else {
            throw new IllegalArgumentException("Invalid service endpoint: " + endpoint);
        }

        // Check if falsely using a fair-share class
        String scheduling_class = cli_props.getProperty(UiOption.SchedulingClass.pname());
        if (scheduling_class != null) {
            DuccSchedulerClasses duccSchedulerClasses = DuccSchedulerClasses.getInstance();
            if (duccSchedulerClasses.isPreemptable(scheduling_class)) {
                throw new IllegalArgumentException("Invalid pre-emptable scheduling class: " + scheduling_class);
            }
        }
        
        // work out stuff I'm dependent upon
        if ( !check_service_dependencies(endpoint) ) {
            throw new IllegalArgumentException("Invalid service dependencies");
        }
        int instances = cli_props.getIntProperty(UiOption.Instances.pname(), 1);
        Trinary autostart = getAutostart();
        String user = (String) cli_props.remove(UiOption.User.pname());
        byte[] auth_block = (byte[]) cli_props.remove(UiOption.Signature.pname());
        
        // A few spurious properties are set as an artifact of parsing the overly-complex command line, and need removal
        cli_props.remove(UiOption.SubmitPid.pname());
        cli_props.remove(UiOption.Register.pname());
        cli_props.remove(UiOption.Instances.pname());
        cli_props.remove(UiOption.Autostart.pname());

        ServiceRegisterEvent ev = new ServiceRegisterEvent(user, instances, autostart, endpoint, cli_props, auth_block, CliVersion.getVersion());

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }
	}

    /**
     * The unregister API is used to unregister a service.  The service manager will stop all instances and 
     * remove the service registration.
     *
     * @param args String array of arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SERVICES">DUCC CLI reference.</a>
     * @return {@link IServiceReply IServiceReply} object with unregister reply status.
     */
    public IServiceReply unregister(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();
        init(this.getClass().getName(), unregister_options, args, null, dp, callback, "sm");
        

        Pair<Integer, String> id = getId(UiOption.Unregister);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());
        boolean asAdministrator = dp.containsKey(UiOption.RoleAdministrator.pname());

        ServiceUnregisterEvent ev = new ServiceUnregisterEvent(user, id.first(), id.second(), auth_block, CliVersion.getVersion());
        ev.setAdministrative(asAdministrator);
        
        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }

	}

    /**
     * The start API is used to start one or more instances of a registered service.
     * 
     * @param args String array of arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SERVICES">DUCC CLI reference.</a>
     * @return {@link IServiceReply IServiceReply} object with start reply status.
     */
    public IServiceReply start(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();
        init(this.getClass().getName(), start_options, args, null, dp, callback, "sm");        

        Pair<Integer, String> id = getId(UiOption.Start);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());
        boolean asAdministrator = dp.containsKey(UiOption.RoleAdministrator.pname());

        ServiceStartEvent ev = new ServiceStartEvent(user, id.first(), id.second(), auth_block, CliVersion.getVersion());
        ev.setAdministrative(asAdministrator);

        int instances = getInstances(-1);
        //boolean update = getUpdate();

        ev.setInstances(instances);
        //ev.setUpdate(update);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }
    }


    /**
     * The stop API is used to stop one or more service instances.
     *
     * @param args String array of arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SERVICES">DUCC CLI reference.</a>
     * @return {@link IServiceReply IServiceReply} object with stop status.
     */
    public IServiceReply stop(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();
        init(this.getClass().getName(), stop_options, args, null, dp, callback, "sm");

        Pair<Integer, String> id = getId(UiOption.Stop);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());
        boolean asAdministrator = dp.containsKey(UiOption.RoleAdministrator.pname());

        ServiceStopEvent ev = new ServiceStopEvent(user, id.first(), id.second(), auth_block, CliVersion.getVersion());
        ev.setAdministrative(asAdministrator);

        int instances = getInstances(-1);
        //boolean update = getUpdate();

        ev.setInstances(instances);
        //ev.setUpdate(update);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }        
    }

    /**
     * The service 'modify' command is used to change various aspects of a registered service
     * without the need to reregister it.
     *
     * @param args String array of arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SERVICES">DUCC CLI reference.</a>
     * @return {@link IServiceReply IServiceReply} object with modify status.
     */
    public IServiceReply modify(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();

        inhibitDefaults();
        init (this.getClass().getName(), modify_options, args, null, dp, callback, "sm");

        enrichPropertiesForDebug(UiOption.Modify);

        Pair<Integer, String> id = getId(UiOption.Modify);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());
        boolean asAdministrator = dp.containsKey(UiOption.RoleAdministrator.pname());
        dp.remove(UiOption.RoleAdministrator.pname()); 

        ServiceModifyEvent ev = new ServiceModifyEvent(user, id.first(), id.second(), dp, auth_block, CliVersion.getVersion());        
        ev.setAdministrative(asAdministrator);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }
    }

    /**
     * The service 'modify' command is used to change various aspects of a registered service
     * without the need to reregister it.
     *
     * @param args String array of arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SERVICES">DUCC CLI reference.</a>
     * @return {@link IServiceReply IServiceReply} object with modify status.
     */
    public IServiceReply modifyX(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();

        init (this.getClass().getName(), modify_options, args, null, dp, callback, "sm");

        Pair<Integer, String> id = getId(UiOption.Modify);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());

        DuccProperties mods = new SpecificationProperties();        
        ServiceModifyEvent ev = new ServiceModifyEvent(user, id.first(), id.second(), mods, auth_block, CliVersion.getVersion());
        int instances = getInstances(-1);
        Trinary autostart = getAutostart();
        // boolean activate = getActivate();
        String  pingArguments = cli_props.getProperty(UiOption.ServicePingArguments.pname());
        String  pingClass     = cli_props.getProperty(UiOption.ServicePingClass.pname());
        String  pingClasspath = cli_props.getProperty(UiOption.ServicePingClasspath.pname());
        String  pingJvmArgs   = cli_props.getProperty(UiOption.ServicePingJvmArgs.pname());
        String  pingTimeout   = cli_props.getProperty(UiOption.ServicePingTimeout.pname());
        String  pingDoLog     = cli_props.getProperty(UiOption.ServicePingDoLog.pname());

        // modify: if something is modified, indicate the new value.  if no value, then it's not modified.
        
        if ( instances > 0 ) mods.setProperty("instances", Integer.toString(instances));
        switch ( autostart ) {
            case True:  mods.setProperty("autostart", "true"); break;
            case False: mods.setProperty("autostart", "false"); break;
            default:
                break;
        }
//        if ( activate ) mods.setProperty("activate", "true");
//        else            mods.setProperty("activate", "false");

        if ( pingArguments != null ) mods.setProperty("service_ping_arguments", pingArguments);
        if ( pingClass     != null ) mods.setProperty("service_ping_class"    , pingClass);
        if ( pingClasspath != null ) mods.setProperty("service_ping_classpath", pingClasspath);
        if ( pingJvmArgs   != null ) mods.setProperty("service_ping_jvm_args" , pingJvmArgs);
        if ( pingTimeout   != null ) mods.setProperty("service_ping_timeout"  , pingTimeout);
        if ( pingDoLog     != null ) mods.setProperty("service_ping_dolog"    , pingDoLog);
        

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }
    }

    /**
     * The query API is used to query the status of services known to the service manager.
     *
     * @param args String array of arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SERVICES">DUCC CLI reference.</a>
     * @return {@link IServiceReply IServiceReply} object with query results status.
     */
    public IServiceReply query(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();
        init(this.getClass().getName(), query_options, args, null, dp, callback, "sm");

        Pair<Integer, String> id = null;
        String sid = cli_props.getProperty(UiOption.Query.pname()).trim();
        if ( sid == null || sid.equals("") ) { 
            id = new Pair<Integer, String>(-1, null);
        } else {        
            id = getId(UiOption.Query);
        }

        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());

        ServiceQueryEvent ev = new ServiceQueryEvent(user, id.first(), id.second(), auth_block, CliVersion.getVersion());

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }
    }

    public IServiceReply observeReferences(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();
        init(this.getClass().getName(), observe_options, args, null, dp, callback, "sm");

        Pair<Integer, String> id = getId(UiOption.Observe);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());
        boolean asAdministrator = dp.containsKey(UiOption.RoleAdministrator.pname());

        ServiceObserveEvent ev = new ServiceObserveEvent(user, id.first(), id.second(), auth_block, CliVersion.getVersion());
        ev.setAdministrative(asAdministrator);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }        
    }

    public IServiceReply ignoreReferences(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();
        init(this.getClass().getName(), ignore_options, args, null, dp, callback, "sm");

        Pair<Integer, String> id = getId(UiOption.Ignore);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());
        boolean asAdministrator = dp.containsKey(UiOption.RoleAdministrator.pname());

        ServiceIgnoreEvent ev = new ServiceIgnoreEvent(user, id.first(), id.second(), auth_block, CliVersion.getVersion());
        ev.setAdministrative(asAdministrator);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }        
    }

    public IServiceReply enable(String[] args)
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();
        init(this.getClass().getName(), enable_options, args, null, dp, callback, "sm");

        Pair<Integer, String> id = getId(UiOption.Enable);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());
        boolean asAdministrator = dp.containsKey(UiOption.RoleAdministrator.pname());

        ServiceEnableEvent ev = new ServiceEnableEvent(user, id.first(), id.second(), auth_block, CliVersion.getVersion());
        ev.setAdministrative(asAdministrator);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }        
    }

    public IServiceReply disable(String[] args)    
        throws Exception
    {
        DuccProperties dp = new SpecificationProperties();
        init(this.getClass().getName(), disable_options, args, null, dp, callback, "sm");

        Pair<Integer, String> id = getId(UiOption.Disable);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());
        boolean asAdministrator = dp.containsKey(UiOption.RoleAdministrator.pname());

        ServiceDisableEvent ev = new ServiceDisableEvent(user, id.first(), id.second(), auth_block, CliVersion.getVersion());
        ev.setAdministrative(asAdministrator);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }        
    }

    void help()
    {
        CommandLine cl;

        cl = new CommandLine(null, registration_options);
        System.out.println(cl.formatHelp("--------------- Registr Options -------------"));

        cl = new CommandLine(null, unregister_options);
        System.out.println(cl.formatHelp("\n\n------------- Unregister Options ------------------"));

        cl = new CommandLine(null, start_options);
        System.out.println(cl.formatHelp("\n\n------------- Start Options ------------------"));

        cl = new CommandLine(null, stop_options);
        System.out.println(cl.formatHelp("\n\n------------- Stop Options ------------------"));

        cl = new CommandLine(null, modify_options);
        System.out.println(cl.formatHelp("\n\n------------- Modify Options ------------------"));

        cl = new CommandLine(null, modify_options);
        System.out.println(cl.formatHelp("\n\n------------- Query Options ------------------"));

        System.exit(1);
    }

    public boolean execute() { return false; }

    static boolean format_reply(UiOption verb, IServiceReply reply)
    {
        // Note
        String ep = reply.getEndpoint()!=null ? reply.getEndpoint() : "";
        String id = reply.getId()!=-1 ? " ID["+String.valueOf(reply.getId())+"]" : "";
        String result = (reply.getReturnCode()) ? " succeeded - " : " failed - ";
        String msg = "Service " + verb + result + reply.getMessage() + " - " + ep + id;
        switch ( verb ) {
           case Register:
           case Unregister:
           case Start:
           case Stop:
           case Modify:
           case Disable:
           case Enable:
           case Ignore:
           case Observe:
               System.out.println(msg);
               break;
           case Query:
               if (reply.getReturnCode()) {
                   System.out.println(reply.toString());
               } else {
                   System.out.println(msg);
               }
               break;
        }

        return reply.getReturnCode();
    }

    static boolean Register(String[] args)
    	throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.register(args);
        return format_reply(UiOption.Register, reply);
    }

    static boolean Unregister(String[] args)
    	throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.unregister(args);
        return format_reply(UiOption.Unregister, reply);
    }

    static boolean Start(String[] args)
        throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.start(args);
        return format_reply(UiOption.Start, reply);
    }

    static boolean Stop(String[] args)
        throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.stop(args);
        return format_reply(UiOption.Stop, reply);
    }

    static boolean Modify(String[] args)
        throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.modify(args);
        return format_reply(UiOption.Modify, reply);
    }

    static boolean Enable(String[] args)
        throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.enable(args);
        return format_reply(UiOption.Enable, reply);
    }

    static boolean Disable(String[] args)
        throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.disable(args);
        return format_reply(UiOption.Disable, reply);
    }

    static boolean ObserveReferences(String[] args)
        throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.observeReferences(args);
        return format_reply(UiOption.Observe, reply);
    }

    static boolean IgnoreReferences(String[] args)
        throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.ignoreReferences(args);
        return format_reply(UiOption.Ignore, reply);
    }

    static boolean Query(String[] args)
        throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.query(args);
        return format_reply(UiOption.Query, reply);
    }

    static void Help()
    {
        DuccServiceApi api = new DuccServiceApi(null);
        api.help();
        System.exit(1);
    }

    static UiOption getVerb(String[] args)
    {
        // need to scan args for the verb, and insure only one verb
        UiOption[] verbs = {
            UiOption.Register, 
            UiOption.Modify, 
            UiOption.Start, 
            UiOption.Stop, 
            UiOption.Query, 
            UiOption.Unregister,
            UiOption.Observe,
            UiOption.Ignore,
            UiOption.Enable,
            UiOption.Disable
        };        
        List<UiOption> check = new ArrayList<UiOption>();
        UiOption reply = UiOption.Help;

        for ( String s : args ) {
            if ( ! s.startsWith("--") ) continue;
            s = s.substring(2);

            if ( s.equals("help") ) Help();

            for ( UiOption v : verbs ) {
                if ( s.equals(v.pname() ) ) {
                    reply = v;
                    check.add(v);
                }
            }
        }

        if ( check.size() > 1 ) {
            String msg = "";
            for ( UiOption o : check ) {
                msg = msg + " " + o;
            }
            throw new IllegalArgumentException("Duplicate service actions: " + msg );
        }

        return reply;
    }


    /*
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
     *  e.g.  UIMA-AS@FixedSleepAE@tcp://node1
     */

    /**
     * This is the main entrypoint, used by the executable jars and callable directly from the command line.
     *
     * If the invocation is successful, the process exits with return code 0.  Otherwise, it exit
     * with return code 1.
     *
     * @param args arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_SERVICES">DUCC CLI reference.</a>
     */
	public static void main(String[] args) 
    {        
        boolean rc = false;
        try {
            switch ( getVerb(args) ) {
                case Register:
                    rc = Register(args);
                    break;
                case Unregister:
                    rc = Unregister(args);
                    break;
                case Start:
                    rc = Start(args);
                    break;
                case Stop:
                    rc = Stop(args);
                    break;
                case Observe:
                    rc = ObserveReferences(args);
                    break;
                case Ignore:
                    rc = IgnoreReferences(args);
                    break;
                case Enable:
                    rc = Enable(args);
                    break;
                case Disable:
                    rc = Disable(args);
                    break;
                case Modify:
                    rc = Modify(args);
                    break;
                case Query:
                    rc = Query(args);
                    break;
                default:
                    System.out.println("Missing service action (register, unregister, start, stop, modify, observe_refrences, ignore_references, enable, disable, or query)");
                    System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("Service call failed: " + e);
            for (String arg : args) {
                if (arg.equals("--debug")) {
                    e.printStackTrace();
                    break;
                }
            }
            System.exit(1);            
        }
        System.exit(rc ? 0 : 1);
	}
	
}
