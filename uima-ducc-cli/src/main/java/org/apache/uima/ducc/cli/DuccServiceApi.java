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
import java.util.Properties;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.transport.event.ServiceModifyEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryEvent;
import org.apache.uima.ducc.transport.event.ServiceRegisterEvent;
import org.apache.uima.ducc.transport.event.ServiceStartEvent;
import org.apache.uima.ducc.transport.event.ServiceStopEvent;
import org.apache.uima.ducc.transport.event.ServiceUnregisterEvent;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceType;
import org.apache.uima.ducc.transport.event.sm.IService.Trinary;
import org.apache.uima.ducc.transport.event.sm.IServiceReply;


/**
 * DUCC service API
 */

public class DuccServiceApi 
    extends CliBase
{

    String sm_port = "ducc.sm.http.port";
    String sm_host = "ducc.sm.http.node";
    String endpoint = null;
    IDuccCallback callback = null;

    UiOption[] registration_options_release = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Description,
        UiOption.SchedulingClass,
        UiOption.LogDirectory,
        UiOption.WorkingDirectory,
        UiOption.Jvm,
        UiOption.JvmArgs,
        UiOption.Classpath,
        UiOption.Environment,
        UiOption.ProcessMemorySize,
        UiOption.ProcessDD,
        UiOption.ProcessFailuresLimit,
        UiOption.ClasspathOrder,
        // UiOption.Specification          // not used for registration
        UiOption.ServiceDependency,
        UiOption.ServiceRequestEndpoint,
        UiOption.ServiceLinger,
        UiOption.ServicePingClass,
        UiOption.ServicePingClasspath,
        UiOption.ServicePingJvmArgs,
        UiOption.ServicePingTimeout,
        UiOption.ServicePingDoLog,

        UiOption.Register,
        UiOption.Autostart,
        UiOption.Instances,
    }; 

    UiOption[] registration_options_beta = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Description,
        UiOption.SchedulingClass,
        UiOption.LogDirectory,
        UiOption.WorkingDirectory,
        UiOption.Jvm,
        UiOption.JvmArgs,
        UiOption.Classpath,
        UiOption.Environment,
        UiOption.ProcessJvmArgs,
        UiOption.ProcessClasspath,
        UiOption.ProcessEnvironment,
        UiOption.ProcessMemorySize,
        UiOption.ProcessDD,
        UiOption.ProcessFailuresLimit,
        UiOption.ClasspathOrder,
        // UiOption.Specification          // not used for registration
        UiOption.ServiceDependency,
        UiOption.ServiceRequestEndpoint,
        UiOption.ServiceLinger,
        UiOption.ServicePingClass,
        UiOption.ServicePingClasspath,
        UiOption.ServicePingJvmArgs,
        UiOption.ServicePingTimeout,
        UiOption.ServicePingDoLog,

        UiOption.Register,
        UiOption.Autostart,
        UiOption.Instances,
    }; 
    
    UiOption[] registration_options = registration_options_release;
    
    UiOption[] unregister_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Unregister,
    }; 


    UiOption[] start_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Start,
        UiOption.Instances,
        UiOption.Update,
    }; 

    UiOption[] stop_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Stop,
        UiOption.Instances,
        UiOption.Update,
    }; 

    UiOption[] modify_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Modify,
        UiOption.Instances,
        UiOption.Autostart,
        UiOption.Activate,
    }; 

    UiOption[] query_options = {
        UiOption.Help,
        UiOption.Debug,
        UiOption.Query,
    }; 

    public DuccServiceApi(IDuccCallback cb)
    {
        this.callback = cb;
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

    private boolean getActivate()
    {
        return cli_props.containsKey(UiOption.Activate.pname());
    }

    private boolean getUpdate()
    {
        return cli_props.containsKey(UiOption.Update.pname());
    }

    private void setLinger()
    {
        String default_linger = ducc_properties.getStringProperty("ducc.sm.default.linger", "5000");
        String linger         = cli_props.getStringProperty(UiOption.ServiceLinger.pname(), default_linger);
        try {             
            @SuppressWarnings("unused")
			long actual = Long.parseLong(linger); // make sure it's a long, don't care about the value
        } catch ( NumberFormatException e ) {
            throw new IllegalArgumentException(UiOption.ServiceLinger.pname() + " is not numeric: " + linger);
        }
    }

    String extractEndpoint(Properties jvmargs)
    {
        // If claspath is not specified, pick it up from the submitter's environment
    	String key_cp = UiOption.ProcessClasspath.pname();
        if ( cli_props.containsKey(UiOption.Classpath.pname()) ) {
        	key_cp = UiOption.Classpath.pname();
        }
        String classpath = cli_props.getStringProperty(key_cp, System.getProperty("java.class.path"));
        cli_props.setProperty(key_cp, classpath);
        
        // No endpoint, resolve from the DD.
        String dd = cli_props.getStringProperty(UiOption.ProcessDD.pname()); // will throw if can't find the prop
        String working_dir = cli_props.getStringProperty(UiOption.WorkingDirectory.pname());
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
    public IServiceReply register(String[] args)
        throws Exception
    {
        DuccProperties dp = new DuccProperties();
        init();
        if(DuccUiUtilities.isSupportedBeta()) {
        	registration_options = registration_options_beta;
        }
        init(this.getClass().getName(), registration_options, args, dp, sm_host, sm_port, "sm", callback, "services");

        //
        // Now: get jvm args and resolve placeholders, in particular, the broker url
        //
        String key_ja = UiOption.ProcessJvmArgs.pname();
        if ( cli_props.containsKey(UiOption.JvmArgs.pname()) ) {
        	key_ja = UiOption.JvmArgs.pname();
        }
        String jvmarg_string = cli_props.getProperty(key_ja);
        Properties jvmargs = DuccUiUtilities.jvmArgsToProperties(jvmarg_string);
        DuccUiUtilities.resolvePropertiesPlaceholders(cli_props, jvmargs);

        setLinger();

        //
        // Establish my endpoint
        //
        String  endpoint = cli_props.getStringProperty(UiOption.ServiceRequestEndpoint.pname(), null);
        if ( endpoint == null ) {               // not custom ... must be uima-as (or fail)

            endpoint = extractEndpoint(jvmargs);

        } else if ( endpoint.startsWith(ServiceType.Custom.decode()) ) {

            // must have a pinger specified
            if ( ! cli_props.containsKey(UiOption.ServicePingClass.pname()) ) {
                throw new IllegalArgumentException("Custom service is missing ping class name.");
            }
 
            String classpath = cli_props.getStringProperty(UiOption.ServicePingClasspath.pname(), System.getProperty("java.class.path"));            
            cli_props.setProperty(UiOption.ServicePingClasspath.pname(), classpath);

        } else if ( endpoint.startsWith(ServiceType.UimaAs.decode()) ) {

            // Infer the classpath (DuccProperties will return the default if the value isn't found.)
        	String key_cp = UiOption.ProcessClasspath.pname();
            if ( cli_props.containsKey(UiOption.Classpath.pname()) ) {
            	key_cp = UiOption.Classpath.pname();
            }
            String classpath = cli_props.getStringProperty(key_cp, System.getProperty("java.class.path"));
            cli_props.setProperty(key_cp, classpath);

            // Given ep must match inferred ep. Use case: an application is wrapping DuccServiceApi and has to construct
            // the endpoint as well.  The app passes it in and we insure that the constructed endpoint matches the one
            // we extract from the DD - the job will fail otherwise, so we catch this early.
            String verify_endpoint = extractEndpoint(jvmargs);
            if ( !verify_endpoint.equals(endpoint) ) {
                throw new IllegalArgumentException("Endpoint from --service_request_endpoint does not match endpoint ectracted from UIMA DD" 
                                                   + "\n--service_request_endpoint: " + endpoint 
                                                   + "\nextracted:                : " + verify_endpoint );
            }
        } else {
            throw new IllegalArgumentException("Invalid service endpoint: " + endpoint);
        }

        // work out stuff I'm dependendent upon
        resolve_service_dependencies(endpoint);
        int instances = cli_props.getIntProperty(UiOption.Instances.pname(), 1);
        Trinary autostart = getAutostart();
        String user = (String) dp.remove(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.remove(UiOption.Signature.pname());
        
        // A few spurious properties are set as an artifact of parsing the overly-complex command line, and need removal
        dp.remove(UiOption.SubmitPid.pname());
        dp.remove(UiOption.Register.pname());
        dp.remove(UiOption.Instances.pname());
        dp.remove(UiOption.Autostart.pname());

        ServiceRegisterEvent ev = new ServiceRegisterEvent(user, instances, autostart, endpoint, cli_props, auth_block);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }
	}

    /**
     * @param id The full service id as returned by register
     * @return 
     */
    public IServiceReply unregister(String[] args)
        throws Exception
    {
        DuccProperties dp = new DuccProperties();
        init(this.getClass().getName(), unregister_options, args, dp, sm_host, sm_port, "sm", callback, "services");

        Pair<Integer, String> id = getId(UiOption.Unregister);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());

        ServiceUnregisterEvent ev = new ServiceUnregisterEvent(user, id.first(), id.second(), auth_block);
        
        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }

	}

    /**
     * @param props Name of file in standard Java properies format with the service specification.
     * @return 
     */
    public IServiceReply start(String[] args)
        throws Exception
    {
        DuccProperties dp = new DuccProperties();
        init(this.getClass().getName(), start_options, args, dp, sm_host, sm_port, "sm", callback, "services");

        Pair<Integer, String> id = getId(UiOption.Start);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());

        ServiceStartEvent ev = new ServiceStartEvent(user, id.first(), id.second(), auth_block);

        int instances = getInstances(-1);
        boolean update = getUpdate();

        ev.setInstances(instances);
        ev.setUpdate(update);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }
    }


    /**
     * @param props Name of file in standard Java properies format with the service specification.
     * @return 
     */
    public IServiceReply stop(String[] args)
        throws Exception
    {
        DuccProperties dp = new DuccProperties();
        init(this.getClass().getName(), stop_options, args, dp, sm_host, sm_port, "sm", callback, "services");

        Pair<Integer, String> id = getId(UiOption.Stop);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());

        ServiceStopEvent ev = new ServiceStopEvent(user, id.first(), id.second(), auth_block);

        int instances = getInstances(-1);
        boolean update = getUpdate();

        ev.setInstances(instances);
        ev.setUpdate(update);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }        
    }

    /**
     * @return 
     */
    public IServiceReply modify(String[] args)
        throws Exception
    {
        DuccProperties dp = new DuccProperties();
        init(this.getClass().getName(), modify_options, args, dp, sm_host, sm_port, "sm", callback, "services");

        Pair<Integer, String> id = getId(UiOption.Modify);
        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());

        ServiceModifyEvent ev = new ServiceModifyEvent(user, id.first(), id.second(), auth_block);
        int instances = getInstances(-1);
        Trinary autostart = getAutostart();
        boolean activate = getActivate();

        ev.setInstances(instances);
        ev.setAutostart(autostart);
        ev.setActivate(activate);
        
        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }
    }

    /**
     * @param props Name of file in standard Java properies format with the service specification.
     * @return 
     */
    public IServiceReply query(String[] args)
        throws Exception
    {
        DuccProperties dp = new DuccProperties();
        init(this.getClass().getName(), query_options, args, dp, sm_host, sm_port, "sm", callback, "services");

        Pair<Integer, String> id = null;
        String sid = cli_props.getProperty(UiOption.Query.pname()).trim();
        if ( sid == null || sid.equals("") ) { 
            id = new Pair<Integer, String>(-1, null);
        } else {        
            id = getId(UiOption.Query);
        }

        String user = dp.getProperty(UiOption.User.pname());
        byte[] auth_block = (byte[]) dp.get(UiOption.Signature.pname());

        ServiceQueryEvent ev = new ServiceQueryEvent(user, id.first(), id.second(), auth_block);

        try {
            return (IServiceReply) dispatcher.dispatchAndWaitForDuccReply(ev);
        } finally {
            dispatcher.close();
        }
    }

    void help()
    {
        HelpFormatter formatter = new HelpFormatter();
        Options options;

        formatter.setWidth(IUiOptions.help_width);

        System.out.println("------------- Register Options ------------------");
        options = makeOptions(registration_options, true);
        formatter.printHelp(this.getClass().getName(), options);

        System.out.println("\n\n------------- Unregister Options ------------------");
        options = makeOptions(unregister_options, true);
        formatter.printHelp(this.getClass().getName(), options);

        System.out.println("\n\n------------- Start Options ------------------");
        options = makeOptions(start_options, true);
        formatter.printHelp(this.getClass().getName(), options);

        System.out.println("\n\n------------- Stop Options ------------------");
        options = makeOptions(stop_options, true);
        formatter.printHelp(this.getClass().getName(), options);

        System.out.println("\n\n------------- Modify Options ------------------");
        options = makeOptions(modify_options, true);
        formatter.printHelp(this.getClass().getName(), options);

        System.out.println("\n\n------------- Query Options ------------------");
        options = makeOptions(query_options, true);
        formatter.printHelp(this.getClass().getName(), options);

        System.exit(1);
    }

    boolean execute() { return false; }

    static boolean format_reply(UiOption verb, IServiceReply reply)
    {
        String result = (reply.getReturnCode()) ? "succeeded" : "failed";
        String reason = (reply.getReturnCode()) ? "" : ": " +reply.getMessage();
        String action = "Service " + verb;
        String msg = (action + " " + result + " ID " + ((reply.getId() == -1) ? "<none>" : reply.getId()) + " endpoint " + reply.getEndpoint() + reason);
        switch ( verb ) {
           case Register:
           case Unregister:
           case Start:
           case Stop:
           case Modify:
               System.out.println(msg);
               break;
           case Query:
               System.out.println(reply.toString());
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
            UiOption.Unregister
        };        
        List<UiOption> check = new ArrayList<UiOption>();
        UiOption reply = null; ;

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
            throw new IllegalArgumentException("Duplicate service actions " + msg + ": not allowed.");
        }

        return reply;
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
                case Modify:
                    rc = Modify(args);
                    break;
                case Query:
                    rc = Query(args);
                    break;
                default:
                    System.out.println("Missing service action (register, unregister, start, stop, modify, or query)");
                    System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("Service call failed: " + e.toString());
            System.exit(1);            
        }
        System.exit(rc ? 0 : 1);
	}
	
}
