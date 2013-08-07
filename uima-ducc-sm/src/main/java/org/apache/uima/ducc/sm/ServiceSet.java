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
package org.apache.uima.ducc.sm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.cli.IServiceApi.RegistrationOption;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.cli.UimaAsPing;
import org.apache.uima.ducc.cli.UimaAsServiceMonitor;
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.id.ADuccId;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.sm.IServiceDescription;
import org.apache.uima.ducc.transport.event.sm.ServiceDescription;
import org.apache.uima.util.Level;


/**
 * Represents the collection of process, jobs, and such that implement a given service.
 */

public class ServiceSet
	implements SmConstants
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private DuccLogger logger = DuccLogger.getLogger(this.getClass().getName(), COMPONENT_NAME);	

    // key is unique id of descriptor.  The descriptor inherites key from a Job's DuccId, or from
    // a unique-to SM key for implicit references.
    HashMap<DuccId, JobState> implementors = new HashMap<DuccId, JobState>();

    // key is job/service id, value is same.  it's a map for fast existence check
    HashMap<DuccId, DuccId> references = new HashMap<DuccId, DuccId>();

    // For a registered service, here is my registered id
    DuccId id;
    HashMap<Long, DuccId> friendly_ids = new HashMap<Long, DuccId>();
    String history_key = "work-instances";

    // incoming nodes, for dup checking
    List<ServiceSet> predecessors = new ArrayList<ServiceSet>();
    List<ServiceSet> successors   = new ArrayList<ServiceSet>();

    // for UIMA-AS this is the endpoint as the unique identifier of this service
    String key;

    // UIMA-AS endpoint information
    String endpoint;
    String broker;
    String broker_host;
    int broker_port;
    int broker_jmx_port = 1099;
    String[] independentServices = null;

    // Registered services, the submitter
    String user;

    // Registered services, Automatically start at boot, and keep implementors alive
    boolean autostart = false;
    // Registered services, we've been stopped.  Must remember in order to counteract autostart.
    boolean stopped   = false;
    // Registered services, remember if was started by reference only so we can stop when refs die
    boolean referenced_start = false;

    // Registered services, the number of instances to maintain
    int instances = 1;

    // Service pinger
    IServiceMeta serviceMeta = null;

    // After stopping a pinger we need to discard it,and we usually need to stop it well before
    // it is ok to delete residual state such as UIMA-AS queues.  Instead of discarding it, we
    // stash it here to use once the last implementor seems to be dead.
    IServiceMeta residualMeta = null;

    // registered services state files
    DuccProperties job_props  = null;
    DuccProperties meta_props = null;
    String props_filename = null;
    String meta_filename = null;
    boolean deregistered = false;

    ServiceType  service_type  = ServiceType.Undefined;
    ServiceClass service_class = ServiceClass.Undefined;
    ServiceState service_state = ServiceState.Undefined;;

    // structures to manage service linger after it exits
    Timer timer = null;
    LingerTask linger = null;
    long linger_time = 5000;

    int failure_max = ServiceManagerComponent.failure_max;
    int failure_run = 0;                   // max allowed consecutive failures, current failure count

    //JobState     job_state     = JobState.Undefined;
    //
    // This is the constructor for an implicit service
    //
    public ServiceSet(String key, DuccId id)
    {
        this.key = key;
        this.id = id;
        parseEndpoint(key);

        if ( this.service_type == ServiceType.Custom ) {
            throw new IllegalStateException("Custom services may not be referenced as Implicit services.");
        }

        this.service_type = ServiceType.UimaAs;
        this.service_class = ServiceClass.Implicit;

        String state_dir = System.getProperty("DUCC_HOME") + "/state";

        // Need job props and meta props for webserver.
        // The pinger is always the default configured UIMA-AS pinger.
        //
        // job props: working_directory, log_directory
        // meta props: endpoint, user
        job_props = new DuccProperties();
        // job_props.put("working_directory", System.getProperty("user.dir")); // whatever my current dir is
        // job_props.put("log_directory", System.getProperty("user.dir") + "/../logs");
        //job_props.put("service_ping_jvm_args", "-Xmx50M");
        props_filename = state_dir + "/services/" + id.toString() + ".svc";
        saveServiceProperties();

        meta_props = new DuccProperties();
        meta_props.put("user", System.getProperty("user.name"));
        meta_props.put("endpoint", key);
        meta_props.put("service-class", ""+service_class.decode());
        meta_props.put("service-type", ""+service_type.decode());
        meta_props.put("stopped", ""+stopped);
        meta_props.put("service-state", ""+getServiceState());
        meta_props.put("ping-active", "false");
        meta_props.put("service-alive",      "false");
        meta_props.put("service-healthy",    "false");
        meta_props.put("service-statistics", "N/A");
        meta_props.put("ping-only", "true");

        meta_filename = state_dir + "/services/" + id.toString() + ".meta";
        saveMetaProperties();
    }

    //
    // Constructor for a submitted service
    //
    public ServiceSet(DuccId id, DuccId first_implementor, String key, String[] independentServices)
    {
        this.key = key;
        this.id = id;
        this.friendly_ids.put(first_implementor.getFriendly(), null);

        this.independentServices = independentServices;
        this.service_class = ServiceClass.Submitted;        

        parseEndpoint(key);

        String state_dir = System.getProperty("DUCC_HOME") + "/state";
        // Need job props and meta props for webserver.
        // The pinger is always the default configured UIMA-AS pinger.
        // Submitted services must always be UIMA-AS services, for now, checked in caller.
        //
        // job props: working_directory, log_directory
        // meta props: endpoint, user
        job_props = new DuccProperties();
        //job_props.put("service_ping_jvm_args", "-Xmx50M");
        props_filename = state_dir + "/services/" + id.toString() + ".svc";
        saveServiceProperties();

        meta_props = new DuccProperties();
        meta_props.put("user", System.getProperty("user.name"));
        meta_props.put("endpoint", key);
        meta_props.put("service-class", ""+service_class.decode());
        meta_props.put("service-type", ""+service_type.decode());
        meta_props.put("stopped", ""+stopped);
        meta_props.put("service-state", ""+getServiceState());
        meta_props.put("ping-active", "false");
        meta_props.put("service-alive",      "false");
        meta_props.put("service-healthy",    "false");
        meta_props.put("service-statistics", "N/A");
        meta_props.put("implementors", ""+id.getFriendly());
        meta_props.put("ping-only", "false");

        meta_filename = state_dir + "/services/" + id.toString() + ".meta";
        saveMetaProperties();
    }

    //
    // Constructor for a registered service
    //
    public ServiceSet(DuccId id, String props_filename, String meta_filename, DuccProperties props, DuccProperties meta)
    {
        this.job_props = props;
        this.meta_props = meta;
        this.id = id;
        this.props_filename = props_filename;
        this.meta_filename = meta_filename;
        this.service_state = ServiceState.NotAvailable;
        this.linger_time = props.getLongProperty(RegistrationOption.ServiceLinger.decode(), 5000);
        this.key = meta.getProperty("endpoint");
        this.failure_max = props.getIntProperty(UiOption.ProcessFailuresLimit.pname(), ServiceManagerComponent.failure_max);

        parseEndpoint(key);

        this.user = meta.getProperty("user");
        this.instances = meta.getIntProperty("instances", 1);
        this.autostart = meta.getBooleanProperty("autostart", false);

        String idprop  = meta.getProperty("implementors", null);
        if ( idprop != null ) {
            String[] ids = idprop.split("\\s");
            for ( String i : ids ) {
                friendly_ids.put(Long.parseLong(i), null);
            }
        }
        this.service_class = ServiceClass.Registered;

        parseIndependentServices();

        if ( ! job_props.containsKey("service_ping_dolog")) {
            job_props.put("service_ping_dolog", "false");
        }        
        if ( !job_props.containsKey("service_ping_timeout") ) {
            job_props.put("service_ping_timeout", ""+ServiceManagerComponent.meta_ping_timeout);
        }

        meta_props.put("service-class", ""+service_class.decode());
        meta_props.put("service-type", ""+service_type.decode());
        meta_props.put("stopped", ""+stopped);
        meta_props.put("service-state", ""+getServiceState());
        meta_props.put("ping-active", "false");
        meta_props.put("service-alive",      "false");
        meta_props.put("service-healthy",    "false");
        meta_props.put("service-statistics", "N/A");

        if ( isStartable() ) {
            meta_props.put("ping-only", "false");
        } else {
            meta_props.put("ping-only", "true");
        }
        // caller will save the meta props, **if** the rest of registration is ok.

        //UIMAFramework.getLogger(BaseUIMAAsynchronousEngineCommon_impl.class).setLevel(Level.OFF);
        //UIMAFramework.getLogger(BaseUIMAAsynchronousEngine_impl.class).setLevel(Level.OFF);
        // there are a couple junky messages that slip by the above configurations.  turn the whole danged thing off.
        UIMAFramework.getLogger().setLevel(Level.OFF);
    }

    DuccId getId()
    {
        return id;
    }

    protected void parseEndpoint(String ep)
    {
        if ( ep.startsWith(ServiceType.UimaAs.decode()) ) {
            int ndx = ep.indexOf(":");
            ep = ep.substring(ndx+1);
            ndx = ep.indexOf(":");

            this.endpoint = ep.substring(0, ndx).trim();
            this.broker = ep.substring(ndx+1).trim();
            this.service_type = ServiceType.UimaAs;

            URL url = null;
  			try {                
				url = new URL(null, broker, new TcpStreamHandler());
			} catch (MalformedURLException e) {
                throw new IllegalArgumentException("Invalid broker URL: " + broker);
			}
            this.broker_host = url.getHost();
            this.broker_port = url.getPort();

            if ( this.endpoint.equals("") || this.broker.equals("") ) {
                throw new IllegalArgumentException("The endpoint cannot be parsed.  Expecting UIMA-AS:Endpoint:Broker, received " + key);
            }
        } else {
            this.service_type = ServiceType.Custom;
            int ndx = ep.indexOf(":");
            this.endpoint = ep.substring(ndx+1);
        }

    }

    synchronized void deleteProperties()
    {

        // be sure to move any services that seem not to have croaked yet to history
        String history = meta_props.getStringProperty(history_key, "");
        for ( Long id : friendly_ids.keySet() ) {
            history = history + " " + id.toString();
        }
        meta_props.put(history_key, history);
        meta_props.remove("implementors");

        ServiceManagerComponent.deleteProperties(id.toString(), meta_filename, meta_props, props_filename, job_props);
        meta_filename = null;
        props_filename = null;
    }

    void setIncoming(ServiceSet sset)
    {
        predecessors.add(sset);
    }

    void clearEdges()
    {
        predecessors.clear();
        successors.clear();
    }

    boolean isStopped()
    {
        return stopped;
    }

    boolean hasPredecessor()
    {
        return predecessors.size() != 0;
    }

    List<ServiceSet> getPredecessors()
    {
        return predecessors;
    }

    void removePredecessor(ServiceSet pred)
    {
        predecessors.remove(pred);
    }

    void setOutgoing(ServiceSet sset)
    {
        this.successors.add(sset);
    }

    List<ServiceSet> getSuccessors()
    {
        return new ArrayList<ServiceSet>(this.successors);
    }

    void removeSuccessor(ServiceSet succ)
    {
        successors.remove(succ);
    }

    boolean hasSuccessor()
    {
        return successors.size() != 0;
    }

    String[] getIndependentServices()
    {
        return independentServices;
    }

    // just for testing!
    void setIndependentServices(String[] ind)
    {
        this.independentServices = ind;
    }

    private void parseIndependentServices()
    {
        String depstr = job_props.getProperty(RegistrationOption.ServiceDependency.decode());
        String[] result = null;

        if ( depstr != null ) {
            result = depstr.split("\\s");
            for ( int i = 0; i < result.length; i++ ) {
                result[i] = result[i].trim();
            }
        }
        independentServices = result;
    }

    /**
     * A service is startable if it's UIMA-AS, or if it's CUSTOM and has a process_executable
     * associated with it.  A non-startable CUSTOM service may have only a pinger.
     */
    boolean isStartable()
    {
        switch ( service_type ) {
            case UimaAs:
                return true;
            case Custom: 
                return job_props.containsKey("process_executable");
        }
        return false;  // redundant but needed for compilation
    }

    /**
     * At boot only ... synchronize my state with published OR state.
     */
    void synchronizeImplementors(Map<DuccId, JobState> work)
    {
        HashMap<Long, DuccId> newmap = new HashMap<Long, DuccId>();
        // first loop synchronized 'friendly_ids' with live jobs comining in
        for ( DuccId id : work.keySet() ) {

            long fid = id.getFriendly();
            if ( friendly_ids.containsKey(fid) ) {
                JobState js = work.get(id);
                implementors.put(id, js);
                newmap.put(fid, id);
            }
        }

        // second loop synchronizes history with jobs that used to be live and aren't any more (because of restart)
        String history = meta_props.getStringProperty(history_key, "");
        for ( Long friendly : friendly_ids.keySet() ) {
            DuccId id = newmap.get(friendly);
            if ( id == null ) {
                history = history + " " + friendly;
            }
        }
        meta_props.put(history_key, history);

        friendly_ids = newmap;                       // replace persisted version with validated version from OR state
        persistImplementors();
    }

    /**
     *
     */
    void enforceAutostart()
    {
    	//String methodName = "enforceAutostart";
        if ( ! autostart ) return;                   // not doing auto, nothing to do
        if ( stopped     ) return;                   // doing auto, but we've been manually stopped
        if ( failure_run >= failure_max ) return;    // too  many failures, no more enforcement

        if ( (!isStartable()) && (serviceMeta == null) ) {    // ping-only and pinger not alive
            start();                                          // ... then it needs to be started
            return;
        }

        // could have more implementors than instances if some were started dynamically but the count not persisted
        int needed = Math.max(0, instances - countImplementors());

        while ( (needed--) > 0 ) {
            if ( ! start() ) break;
        }
    }

    /**
     * Add implementor that we have an OR-assigned DuccId for
     */
    public void addImplementor(DuccId id, JobState js)
    {
        if ( isSubmitted() ) {
            friendly_ids.put(id.getFriendly(), id);
        }
        implementors.put(id, js);
        persistImplementors();
    }

    void promote()
    {
        String methodName = "promote";
        logger.debug(methodName, null, "Promoting", key);
        switch ( service_class ) {
            case Implicit : this.service_class = ServiceClass.Submitted; break;
            case Submitted: break;
            default       : throw new IllegalStateException("Trying to promote a Registered service!");
        }
    }

    boolean isUimaAs()
    {
        return (service_type == ServiceType.UimaAs);
    }

    boolean isCustom()
    {
        return (service_type == ServiceType.Custom);
    }

    Map<DuccId, JobState> getImplementors()
    {
        return implementors;
    }

    DuccProperties getJobProperties()
    {
        return job_props;
    }

    DuccProperties getMetaProperties()
    {
        return meta_props;
    }

    boolean isImplicit()
    {
        return (service_class == ServiceClass.Implicit);
    }

    boolean isSubmitted()
    {
        return (service_class == ServiceClass.Submitted);
    }

    boolean isPingOnly()
    {
        return meta_props.containsKey("ping-only");
    }

    boolean isRegistered()
    {
        return (service_class == ServiceClass.Registered) && (!deregistered);
    }

    void setReferencedStart(boolean val)
    {
        this.referenced_start = val;
    }

    boolean isReferencedStart()
    {
        return this.referenced_start;
    }

    String getUser()
    {
        return user;
    }

    /**
     * True only if
     *   a) this is a registereed service, and
     *   b) it has been deregistered.
     * because you can't deregister a non-registered service.
     */
    boolean isDeregistered()
    {
        return (service_class == ServiceClass.Registered) && (deregistered);
    }

    void deregister()
    {
        deregistered = true;
    }

    String getMetaFilename()
    {
        return meta_filename;
    }

    String getPropsFilename()
    {
        return props_filename;
    }

    synchronized int getNInstances()
    {
        return instances;
    }

    synchronized void saveMetaProperties()
    {
        String methodName = "saveMetaProperties";

        if ( meta_filename == null ) {
            // if this is null it was deleted and this is some kind of lingering thread updating, that
            // we don't really want any more
            logger.warn(methodName, id, "Meta properties is deleted, bypassing attempt to save.");
            return;
        }

        meta_props.put("stopped", ""+stopped);
        meta_props.put("service-state", ""+ getServiceState());
        meta_props.put("ping-active", "" + (serviceMeta != null));
        meta_props.put("service-alive",      "false");
        meta_props.put("service-healthy",    "false");
        meta_props.put("service-statistics", "N/A");
        
        if ( serviceMeta != null ) {
            IServiceStatistics ss = serviceMeta.getServiceStatistics();
            if ( ss != null ) {
                meta_props.put("service-alive",      "" + ss.isAlive());
                meta_props.put("service-healthy",    "" + ss.isHealthy());
                meta_props.put("service-statistics", "" + ss.getInfo());
            }
        }
        
        FileOutputStream fos = null;
        try {            
			fos = new FileOutputStream(meta_filename);            
			meta_props.store(fos, "Meta descriptor");
		} catch (FileNotFoundException e) {
            logger.warn(methodName, id, "Cannot save meta properties, file does not exist.");
		} catch (IOException e) {
            logger.warn(methodName, id, "I/O Error saving meta properties:", e);
		} finally {
            try {
				if ( fos != null ) fos.close();
			} catch (IOException e) {
			}
        }
        return;
    }

    void saveServiceProperties()
    {
        String methodName = "saveServiceProperties";
        FileOutputStream fos = null;
        try {
        	fos = new FileOutputStream(props_filename);
			job_props.store(fos, "Service descriptor");
		} catch (FileNotFoundException e) {
            logger.warn(methodName, id, "Cannot save service properties, file does not exist.");
		} catch (IOException e) {
            logger.warn(methodName, id, "I/O Error saving service properties:", e);
		} finally {
            try {
				if ( fos != null ) fos.close();
			} catch (IOException e) {
			}
        }
        return;
    }

    synchronized void setNInstances(int n)
    {
        if ( n != meta_props.getIntProperty("instances") ) {
            meta_props.setProperty("instances", Integer.toString(n));
            this.instances = n;
            saveMetaProperties();
        }
    }

    synchronized void setAutostart(boolean auto)
    {
        meta_props.setProperty("autostart", auto ? "true" : "false");
        this.autostart = auto;
        saveMetaProperties();
        if ( auto ) {
            // turning this on gives benefit of the doubt on failure management
            failure_run = 0;
        }
    }

    synchronized boolean isAutostart()
    {
        return autostart;
    }

    synchronized void persistImplementors()
    {
        if ( isImplicit() ) return;

        if ( friendly_ids.size() == 0 ) {
            meta_props.remove("implementors");
        } else {
            StringBuffer sb = new StringBuffer();
            for ( Long l : friendly_ids.keySet() ) {
                sb.append(Long.toString(l));
                sb.append(" ");
            }
            String s = sb.toString().trim();
            meta_props.setProperty("implementors", s);
        }
        saveMetaProperties();
    }

    synchronized void persistReferences()
    {
        if ( references.size() == 0 ) {
            meta_props.remove("references");
        } else {
            StringBuffer sb = new StringBuffer();
            for ( DuccId id : references.keySet() ) {
                sb.append(id.toString());
                sb.append(" ");
            }
            String s = sb.toString().trim();
            meta_props.setProperty("references", s);
        }
        saveMetaProperties();
    }

    /**
     * If we're are registered service, and one of the "stringids" from submit
     * happens to match the friendly id passed in then this ServiceSet is
     * the representative object for the service.
     */
    boolean matches(DuccId did) 
    {
        if ( ! isRegistered() ) return false;

        if ( friendly_ids.containsKey(did.getFriendly()) ) {
            friendly_ids.put(did.getFriendly(), did);
            return true;
        }

        return false;            
    }

    boolean containsImplementor(DuccId id)
    {
        // must use friendly, in case the thing was just started and not into the implementors set yet
        return friendly_ids.containsKey(id.getFriendly());
    }

    public void removeImplementor(DuccId id)
    {
        String methodName = "removeImplementors";
        if ( ! implementors.containsKey(id ) ) return;  // quick short circuit if it's already gone

        logger.debug(methodName, this.id, "Removing implementor", id);
        implementors.remove(id);
        friendly_ids.remove(id.getFriendly());
        if ( implementors.size() == 0 ) {
            stopPingThread();
        }
        String history = meta_props.getStringProperty(history_key, "");
        history = history + " " + id.toString();
        meta_props.put(history_key, history);
        persistImplementors();

        //
        // So much to check here
        // -  all implementors gone?
        // - is it a registered, not-ping-only service, or a submitted service (startable)
        // - is it a uima-as service, with an internal pinger?
        // Only if all that is true, we'll clear out the queues.
        //==
        logger.debug(methodName, id, "implementors.size()", implementors.size(),
                     "service_class", service_class,
                     "isStartable()", isStartable(),
                     "isSubmitted()", isSubmitted(),
                     "service_type", service_type,
                     "ping_class", job_props.getStringProperty("service_ping_class", UimaAsPing.class.getName())
                     );

        // This block is to clear the service queue from ActiveMq if it's no longer being used.
        if ( implementors.size() == 0 ) {     // Went to 0 and there was a pinger?
            if ( ( (service_class == ServiceClass.Registered) && isStartable()) || isSubmitted() ) {   // Is one of our happy cases (not ping-only, we don't know much about it.)
                if ( service_type == ServiceType.UimaAs ) {
                    // Either no pinger specified, in which case the default is used.  Or, it is specified, and if it
                    // matches the default, we get to clear anyway.
                    String pingclass = job_props.getStringProperty("service_ping_class", UimaAsPing.class.getName());
                    if ( pingclass.equals(UimaAsPing.class.getName()) ) {
                        UimaAsServiceMonitor monitor = new UimaAsServiceMonitor(endpoint, broker_host, broker_jmx_port);
                        logger.debug(methodName, id, "Clearing queues");
                        try {
                        	monitor.init(null);
							monitor.clearQueues();
						} catch (Throwable e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                    }
                }
            }
        }
    }

    public synchronized int countImplementors()
    {
        // The implementos and friendly_ids sets track each other carefully.  The former
        // tracks process state via the remote process's DuccId.  The latter tracks
        // what we've tried to start, but may not know the actual state for until we get
        // Orchestrator publications.
        //
        // To avoid ugly races, we consider a process to be an implementor as soon as we
        // get the "friendly" id from the Orchestrator.
        return friendly_ids.size();
    }

    public synchronized int reference(DuccId id)
    {
        String methodName = "reference";
        logger.debug(methodName, this.id, " ---------------- Service ", this.id, "references", id);

        if ( linger != null ) {
            logger.debug(methodName, this.id, " ---------------- Canceling linger task");
            linger.cancel();
            linger = null;
        }
        references.put(id, id);

        persistReferences();
        return references.size();
    }

    public synchronized int dereference(DuccId id)
    {
        if ( references.remove(id) == null ) {
            throw new IllegalStateException("Id " + id + " not found in map for " + getKey());
        }

        // stop the pinger if no longer needed
        if ( (references.size() == 0) &&                          // nothing left
             ( isImplicit() || ( isCustom() && !isStartable()) )   // implicit UIMA-AS || implicit CUSTOM
             ) 
        {
            stopPingThread();                                     // must stop pinger because there's no state machine
                                                                  // to do it for us in this situation
        }

        persistReferences();
        return references.size();
    }

    public synchronized int countReferences()
    {
        // note that this could change as soon as you get it so don't count on it being correct
        // this is intended only for messages that don't have to be too accurate
        return references.size();
    }

    private ServiceState translateJobState(JobState js)
    {        
        switch ( js ) {
		    case Received:				// Job has been vetted, persisted, and assigned unique Id
		    case WaitingForDriver:		// Process Manager is launching Job Driver
		    case WaitingForServices:	// Service Manager is checking/starting services for Job
                return ServiceState.Waiting;
		    case WaitingForResources:	// Scheduler is assigning resources to Job
		    case Initializing:			// Process Agents are initializing pipelines
                return ServiceState.Initializing;
		    case Running:				// At least one Process Agent has reported process initialization complete
                return ServiceState.Available;
		    case Completing:			// Job processing is completing
		    case Completed:				// Job processing is completed
		    case Undefined:				// None of the above
		    default:
                return ServiceState.NotAvailable;
        }
    }

    /**
     * The MAX of the states of the implementors.
     */
    private ServiceState cumulativeJobState()
    {
        ServiceState response = ServiceState.Undefined;
        for ( JobState s : implementors.values() ) {
            ServiceState translated = translateJobState(s);
            if (  translated.ordinality() > response.ordinality() ) response = translated;
        }
        return response;
    }

    // synchronized void establish()
//     {        
//         if ( implementors.size() == 0 ) {
//             startPingThread();
//         }
//         // Otherwise we let the form that passes in implementor state start the pinger
//     }

    public synchronized void establish()
    {
        String methodName = "establish.0";

        logger.debug(methodName, id, "service_class", service_class, "nimplementors", implementors.size(),
        			"instances", instances);
        switch ( service_class ) {
            case Implicit:
                startPingThread(); 
                break;
            case Submitted:
                for ( DuccId id : implementors.keySet() ) {       // there's only one
                    establish(id, implementors.get(id));
                }
                break;
            case Registered:
                // use friendly to find number of potential instances because it reflects both
                // the number of actually running instances plus those just started but not yet
                // checked with their ducc ids yet.
                
                if ( friendly_ids.size() > 0 ) {
                    for ( DuccId id : implementors.keySet() ) {       // there's only one
                        establish(id, implementors.get(id));
                    }
                } else {
                    if ( service_type == ServiceType.Custom ) {
                        startPingThread();
                    } else {
                        int needed = Math.max(0, instances - friendly_ids.size());
                        while ( (needed--) > 0 ) {
                            start();
                        }
                    }
                }
                break;        
        }        
    }

    /**
     * Starts a ping thread because I've started up and I need to monitor myself.
     */
    public synchronized void establish(DuccId id, JobState job_state)
    {
        String methodName = "establish.1";

        if ( service_class == ServiceClass.Implicit ) {
            startPingThread(); 
            return;
        }

        if ( service_class == ServiceClass.Submitted ) {
            //
            // Annoying edge case - cancel a service, then resubmit before the first one has time
            // to wind down - need to be sure that the winding-down service is not handled by the
            // new instance's state machine.
            //
            if ( ! implementors.containsKey(id) ) {
                logger.debug(methodName, id, "Submitted service: Skipping state machine because the service set has no implemetor for", id);
                return;
            }
        }

        if ( true ) {
            implementors.put(id, job_state);
            ServiceState cumulative = cumulativeJobState();
            //
            // Note on the CUMULATIVE state: this is the cumulative state as determined by service processes.  If they
            // should all die at once through some temporary glitch the state could go to Unavailable even though the
            // SM would now be in active retry - the states below avoid regression state if CUMULATIVE goes to
            // Unavailable but the retry count indicates retry is still in progress.
            //

            //
            // The ping state is pretty much always the right state.  But if we're
            // not yet pinging we need to see if any of the implementors states 
            // indicates we should be pinging, in which case, start the pinger.
            //
            logger.debug(methodName, id, "serviceState", getServiceState(), "cumulativeState", cumulative);
            switch ( getServiceState() ) {
                // If I'm brand new and something is initting then I can be too.  If something is
                // actually running then I can start a pinger which will set my state.
                case Undefined:
                    switch ( cumulative ) {
                        case Initializing:
                            setServiceState(ServiceState.Initializing);
                            break;
                        case Available:
                            startPingThread();
                            break;
                        case Waiting:
                            setServiceState(ServiceState.Waiting);
                            break;
                        case NotAvailable:
                        break;
                    }
                    break;

                    // If I'm initting and now something is running we can start a pinger
                case Initializing:
                    switch ( cumulative ) { 
                        case Available:
                            startPingThread();
                            break;
                        case Initializing:
                            break;
                        case Waiting:
                            setServiceState(ServiceState.Initializing);
                            break;
                        case NotAvailable:
                            if ( failure_run >= failure_max ) {
                                setServiceState(ServiceState.NotAvailable);
                                stopPingThread();
                            } else {
                                // don't regress if we're in retry
                                logger.info(methodName, id, "RETRY RETRY RETRY prevents state regression from Initializing");
                            }
                          break;
                    }
                    break;

                case NotAvailable:
                    switch ( cumulative ) { 
                        case Available:
                            startPingThread();
                            setServiceState(ServiceState.Available);
                            break;
                        case Initializing:
                            setServiceState(ServiceState.Initializing);
                            break;
                        case Waiting:
                            setServiceState(ServiceState.Waiting);
                            break;
                        case NotAvailable:
                            break;
                    }
                    break;

                case Available:
                    switch ( cumulative ) {
                        case Available:
                            startPingThread();
                            break;
                        case Initializing:
                            // Not immediately clear what would cause this other than an error but let's not crash.
                            logger.warn(methodName, id, "STATE REGRESSION:", getServiceState(), "->", cumulative); // can't do anything about it but complain
                            setServiceState(ServiceState.Initializing);
                            break;
                        case NotAvailable:
                            if ( failure_run >= failure_max ) {
                                setServiceState(ServiceState.NotAvailable);
                                stopPingThread();
                            } else {
                                // don't regress if we're in retry
                                logger.info(methodName, id, "RETRY RETRY RETRY prevents state regression from Available");
                            }
                            break;
                    }

                    break;

                case Waiting:                    
                    switch ( cumulative ) {
                        case Available:
                            break;
                        case Initializing:
                            // state regression can happen with a promoted implicit service, where the service is referenced
                            // and a pinger starts before the actual service is available.  the ping keeps us in 'waiting' state
                            // up until expiration, but if the service is actually initializing, we regress.
                            logger.warn(methodName, id, "STATE REGRESSION:", getServiceState(), "->", cumulative); // can't do anything about it but complain
                            setServiceState(ServiceState.Initializing);
                            break;
                        case Waiting:
                            break;
                        case NotAvailable:   
                            if ( failure_run >= failure_max ) {
                                setServiceState(ServiceState.NotAvailable);
                            } else {
                                // don't regress if we're in retry
                                logger.info(methodName, id, "RETRY RETRY RETRY prevents state regression from Waiting");
                            }
                            stopPingThread();
                            break;
                    }
                    break;

                case Stopping:
                    switch ( cumulative ) {
                        case Available:                
                        case Initializing:
                        case Waiting:                            
                            break;

                        case NotAvailable:                
                            // all the implementors died finally
                            setServiceState(ServiceState.NotAvailable);
                            break;
                    }
                    break;                    
            }
        }

    }

    synchronized ServiceState getServiceState()
    {
        return service_state;
    }

    synchronized void setServiceState(ServiceState ss)
    {
        this.service_state = ss;
        saveMetaProperties();
    }

    synchronized String getKey()
    {
        return key;
    }

    void resetRunFailures()
    {
        failure_run = 0;
    }

    synchronized boolean excessiveRunFailures()
    {
        String methodName = "runFailures";
        if ( (++failure_run) >= failure_max ) {
            logger.debug(methodName, id, "RUN FAILURES EXCEEDED");
            return true;
        }
        logger.debug(methodName, id, "RUN FAILURES NOT EXCEEDED YET", failure_run);
        return false;
    }

    private void startPingThread()
    {
    	String methodName = "startPingThread";
        if ( serviceMeta != null ) return;         // don't start multiple times.

        try {
            logger.info(methodName, id, "Starting ping/monitor.");
            serviceMeta = new PingDriver(this);
        } catch ( Throwable t ) {
            logger.error(methodName, id, "Cannot instantiate ping/monitor.", t);
            return;
        }

        setServiceState(ServiceState.Waiting);
        Thread t = new Thread(serviceMeta);
        t.start();
    }

    synchronized void pingExited()
    {
        String methodName = "pingExited";

        if ( serviceMeta != null ) {
            logger.warn(methodName, id, "Pinger exited voluntarily, setting state to Undefined. Endpoint", endpoint);
            setServiceState(ServiceState.Undefined);    // not really sure what state is. it will be

            // checked and updated next run through the
            // main state machine, and maybe ping restarted.
            residualMeta = serviceMeta;
            serviceMeta = null;
        } else {
            setServiceState(ServiceState.NotAvailable);
        }

        if ( isImplicit() ) {
            deleteProperties();
        } else {
            saveMetaProperties();
        }        
    }

    public synchronized void stopPingThread()
    {
        String methodName = "stopPingThread";

        if ( serviceMeta != null ) {
            logger.debug(methodName, id, "Stopping ping thread, endpoint", endpoint);
            serviceMeta.stop();
            residualMeta = serviceMeta;
            serviceMeta = null;
        }

        if ( !isRegistered() ) {
            deleteProperties();
        } else {
            saveMetaProperties();
        }
    }

    synchronized void setResponsive()
    {
        setServiceState(ServiceState.Available);
        saveMetaProperties();
    }

    synchronized void setUnresponsive()
    {
        setServiceState(ServiceState.NotAvailable);
        saveMetaProperties();
    }

    synchronized void setWaiting()
    {
        //
        // Only switch state sometimes ....
        //
        switch ( getServiceState() ) {
            case Available:    
            case NotAvailable: 
            case Undefined:    
                setServiceState(ServiceState.Waiting);
                break;
            case Initializing: 
            case Waiting:      
            case Stopping:     
                break;
        }
    }

//    boolean ping()
//    {
//    	//String methodName = "ping";
//        boolean answer = true;
//
//        // Instantiate Uima AS Client
//        BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
//        Map<String, Object> appCtx = new HashMap<String, Object>();
//        appCtx.put(UimaAsynchronousEngine.ServerUri, broker);
//        appCtx.put(UimaAsynchronousEngine.Endpoint, endpoint);
//        appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, ServiceManagerComponent.meta_ping_timeout);  // 500 ms should be enough to get GetMeta reply
//
//        try {
//            //	this sends GetMeta request and blocks waiting for a reply
//            uimaAsEngine.initialize(appCtx);
//            // logger.info(methodName, null, "Dependent Service Available:", getKey());
//        } catch( ResourceInitializationException e) {
//            //	either broker is down or service not available
//            // logger.error(methodName, null, "Remote service unavailable:", getKey());
//            answer = false;             
//        } finally {
//            uimaAsEngine.stop();
//        }
//
//        return answer;
//    }


    void log_text(String logdir, String text)
    {
    	String methodName = "log_text";
        String[] args = {
            System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
            "-u",
            user,
            "-f",
            logdir + "/service.err.log",
            "-a",
            "--",
            text
        };

        ProcessBuilder pb = new ProcessBuilder(args);
        try {
            Process p = pb.start();
            int rc = p.waitFor();
            logger.debug(methodName, null, "Log start errors returns with rc", rc);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    void log_errors(List<String> outlines, List<String> errlines)
    {

        Date date= new Date();
        String ts = (new Timestamp(date.getTime())).toString();

        String logdir = job_props.getProperty(UiOption.LogDirectory.pname());

        StringBuffer buf = new StringBuffer();

        // header
        buf.append("==========");
        buf.append(" Instance Startup Failure (stdout) ");
        buf.append(ts);
        buf.append(" ========================================\n");

        // stdout
        for ( String s : outlines ) {
            buf.append(s);
            buf.append("\n");
        }
        log_text(logdir, buf.toString());
        
        buf = new StringBuffer();
        buf.append("----------");
        buf.append("(stderr) ");
        buf.append(ts);
        buf.append(" ----------------------------------------\n");
        for ( String s : errlines ) {
            buf.append(s);
            buf.append("\n");
        }
        buf.append("==========");
        buf.append(" End Startup Failure ");
        buf.append(ts);
        buf.append(" ========================================\n");
        log_text(logdir, buf.toString());
    }


    /**
     * This assumes the caller has already verified that I'm a registered service.
     */
    boolean start()
    {
    	String methodName = "start";

        logger.debug(methodName, null, "START START START START START START START START");
        this.stopped = false; 

        if ( ! isStartable() ) {
            establish();  // this will just start the ping thread
            return true;
        }

        // Simple use of ducc_ling, just submit as the user.  The specification will have the working directory
        // and classpath needed for the service, handled by the Orchestrator and Job Driver.
        String[] args = {
            System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
            "-u",
            user,
            "--",
            System.getProperty("ducc.jvm"),
            "-cp",
            System.getProperty("java.class.path"),
            "org.apache.uima.ducc.cli.DuccServiceSubmit",
            "--specification",
            props_filename
        };
            
        for ( int i = 0; i < args.length; i++ ) { 
            if ( i > 0 && (args[i-1].equals("-cp") ) ) {
                // The classpaths can be just awful filling the logs with junk.  It will end up in the agent log
                // anyway so let's inhibit it here.
                logger.debug(methodName, null, "Args[", i, "]: <CLASSPATH>");
            } else {
                logger.debug(methodName, null, "Args[", i, "]:", args[i]);
            }
        }

        ProcessBuilder pb = new ProcessBuilder(args);
        Map<String, String> env = pb.environment();
        env.put("DUCC_HOME", System.getProperty("DUCC_HOME"));

        ArrayList<String> stdout_lines = new ArrayList<String>();
        ArrayList<String> stderr_lines = new ArrayList<String>();
		try {
			Process p = pb.start();

            int rc = p.waitFor();
            logger.debug(methodName, null, "DuccServiceSubmit returns with rc", rc);

            // TODO: we should attache these streams to readers in threads because too much output
            //       can cause blocking, deadlock, ugliness.
			InputStream stdout = p.getInputStream();
			InputStream stderr = p.getErrorStream();
			BufferedReader stdout_reader = new BufferedReader(new InputStreamReader(stdout));
			BufferedReader stderr_reader = new BufferedReader(new InputStreamReader(stderr));
			String line = null;
			while ( (line = stdout_reader.readLine()) != null ) {
			    stdout_lines.add(line);
			}

			line = null;
			while ( (line = stderr_reader.readLine()) != null ) {
			    stderr_lines.add(line);
			}

		} catch (Throwable t) {
			// TODO Auto-generated catch block
            logger.error(methodName, null, t);
		}

        for ( String s : stderr_lines ) {
            logger.info(methodName, id, "Start stderr:", s);
        }

        // That was annoying.  Now search the lines for some hint of the id.
        boolean inhibit_cp = false;
        boolean started = false;
        StringBuffer submit_buffer = new StringBuffer();
        boolean recording = false;
        for ( String s : stdout_lines ) {

            // simple logic to inhibit printing the danged classpath
            if ( inhibit_cp ) {
                inhibit_cp = false;
                logger.info(methodName, id, "<INHIBITED CP>");
            } else {
                logger.info(methodName, id, "Start stdout:", s);
            }

            if ( s.indexOf("-cp") >= 0 ) {
                inhibit_cp = true;
            }

            if ( recording ) {
                submit_buffer.append(s.trim());
                submit_buffer.append(";");
            }
            if ( s.startsWith("1001 Command launching...") ) {
                recording = true;
                continue;
            }

            if ( s.startsWith("Service") && s.endsWith("submitted") ) {
                String[] toks = s.split("\\s");
                long friendly = 0;
                try {
                    friendly = Long.parseLong(toks[1]);
                    friendly_ids.put(friendly, null);
                    persistImplementors();
                    started = true;
                    logger.info(methodName, null, "Request to start service " + id.toString() + " accepted as job ", friendly);
                } catch ( NumberFormatException e ) {
                    logger.warn(methodName, null,  "Request to start service " + id.toString() + " failed, can't interpret response.: " + s);
                }

            }
        }

        boolean rc = true;
        if ( ! started ) {
            logger.warn(methodName, null, "Request to start service " + id.toString() + " failed.");
            meta_props.put("submit_error", submit_buffer.toString());
            setAutostart(false);
            log_errors(stdout_lines, stderr_lines);
        } else {
            meta_props.remove("submit_error");
            setServiceState(ServiceState.Initializing);
        }
        saveMetaProperties();
        logger.debug(methodName, null, "ENDSTART ENDSTART ENDSTART ENDSTART ENDSTART ENDSTART");
        return rc;
    }

    /**
     * This assumes the caller has already verified that I'm a registered service.
     */
    void stopOneProcess(DuccId id)
    {
        String methodName = "stop";
        
        String[] args = {
            System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
            "-u",
            user,
            "--",
            System.getProperty("ducc.jvm"),
            "-cp",
            System.getProperty("java.class.path"),
            "org.apache.uima.ducc.cli.DuccServiceCancel",
            "--id",
            id.toString()
        };
        
        for ( int i = 0; i < args.length; i++ ) { 
            if ( i > 0 && (args[i-1].equals("-cp") ) ) {
                // The classpaths can be just awful filling the logs with junk.  It will end up in the agent log
                // anyway so let's inhibit it here.
                logger.debug(methodName, null, "Args[", i, "]: <CLASSPATH>");
            } else {
                logger.debug(methodName, null, "Args[", i, "]:", args[i]);
            }
        }
        
        ProcessBuilder pb = new ProcessBuilder(args);
        Map<String, String> env = pb.environment();
        env.put("DUCC_HOME", System.getProperty("DUCC_HOME"));
            
        ArrayList<String> stdout_lines = new ArrayList<String>();
        ArrayList<String> stderr_lines = new ArrayList<String>();
        try {
            Process p = pb.start();
                
            int rc = p.waitFor();
            logger.debug(methodName, null, "DuccServiceCancel returns with rc", rc);

            InputStream stdout = p.getInputStream();
            InputStream stderr = p.getErrorStream();
            BufferedReader stdout_reader = new BufferedReader(new InputStreamReader(stdout));
            BufferedReader stderr_reader = new BufferedReader(new InputStreamReader(stderr));

            String line = null;
            while ( (line = stdout_reader.readLine()) != null ) {
                stdout_lines.add(line);
            }
                
            line = null;
            while ( (line = stderr_reader.readLine()) != null ) {
                stderr_lines.add(line);
            }
                
        } catch (Throwable t) {
            // TODO Auto-generated catch block
            logger.error(methodName, null, t);
        }

        boolean inhibit_cp = false;
        for ( String s : stdout_lines ) {
            // simple logic to inhibit printing the danged classpath
            if ( inhibit_cp ) {
                inhibit_cp = false;
                logger.info(methodName, id, "<INHIBITED CP>");
            } else {
                logger.info(methodName, id, "Stop stdout:", s);
            }
            
            if ( s.indexOf("-cp") >= 0 ) {
                inhibit_cp = true;
            }
        }
            
        for ( String s : stderr_lines ) {
            logger.info(methodName, id, "Stop stderr:", s);
        }

        // is this the last implementor?  if so the service is no longer available.
        // should not have to do this, the state should update correctly in the state machine
        //if ( implementors.size() <= 1 ) {
        //    service_state = ServiceState.NotAvailable;
        //}
        //return new ServiceReplyEvent(ServiceCode.OK, "Start service " + id.toString() + " complete.", toks[1], null);            
        
    }

    /**
     * Stop everything
     */
    void stop()
    {
        String methodName = "stop";
        logger.debug(methodName, id, "Stopping all implementors");
        this.stopped = true;
        stopPingThread();
        setServiceState(ServiceState.Stopping);

        if ( ! isStartable() ) {         // CUSTOM ping-only service
            return;
        }

        for ( DuccId id : implementors.keySet() ) {
            stopOneProcess(id);
        }
        saveMetaProperties();
    }

    /**
     * Stop 'count' services.
     * TODO: Put in logic to stop intelligently, i.e. favor processes not yet running
     */
    void stop(int count)
    {
        String methodName = "stop(count)";

        if ( ! isStartable() ) {         // CUSTOM ping-only, only one running, let common code do the honors
            stop();
            return;
        }

        logger.debug(methodName, id, "Stopping", count, "implementors");
        for ( DuccId id: implementors.keySet() ) {
            if ( (count--) > 0 ) {
                stopOneProcess(id);
            } else {
                break;
            }
        }
        saveMetaProperties();
    }


    private class LingerTask
        extends TimerTask
    {
        ServiceSet sset;
        LingerTask(ServiceSet sset)
        {        
            String methodName = "LingerTask.init";
            logger.debug(methodName, id, "Linger starts", linger_time);
            this.sset = sset;
        }

        public void run()
        {
            String methodName = "LingerTask.run";
            logger.debug(methodName, id, "Lingering stop completes.");
            // doesn't matter how its started i think, we have to set this flag off when we stop
            sset.setReferencedStart(false);
            linger = null;
            sset.stop();
        }
    }

    void lingeringStop()
    {
        if ( timer == null ) {
            timer = new Timer();
        }
        linger = new LingerTask(this);
        timer.schedule(linger, linger_time);
    }

    IServiceDescription query()
    {
        IServiceDescription sd = new ServiceDescription();
        
        ArrayList<ADuccId> imp = new ArrayList<ADuccId>();
        for ( DuccId id : implementors.keySet() ) {
            imp.add(id);
        }
        sd.setImplementors(imp);

        ArrayList<ADuccId> ref = new ArrayList<ADuccId>();
        ref.clear();
        for ( DuccId id : references.keySet() ) {
            ref.add(id);
        }
        sd.setReferences(ref);

        sd.setInstances(getNInstances());

        sd.setType(service_type);
        sd.setSubclass(service_class);
        sd.setEndpoint(endpoint);
        sd.setBroker(broker);
        sd.setServiceState(getServiceState());
        //sd.setJobState(job_state);
        sd.setActive(serviceMeta != null);
        sd.setStopped(stopped);
        sd.setAutostart(autostart);
        sd.setLinger(linger_time);
        sd.setId(id);
        sd.setDeregistered(isDeregistered());

        if ( serviceMeta != null ) {
            sd.setQueueStatistics(serviceMeta.getServiceStatistics());
        }

        return sd;
    }


    /**
     * For debugging, so it's easier to identify this guy in the eclipse debugger.
     */
    public String toString()
    {
        return endpoint;
    }


}

/**
    For reference

	public enum JobState {
		Received,				// Job has been vetted, persisted, and assigned unique Id
		WaitingForDriver,		// Process Manager is launching Job Driver
		WaitingForServices,		// Service Manager is checking/starting services for Job
		WaitingForResources,	// Scheduler is assigning resources to Job
		Initializing,			// Process Agents are initializing pipelines
		Running,				// At least one Process Agent has reported process initialization complete
		Completing,				// Job processing is completing
		Completed,				// Job processing is completed
		Undefined				// None of the above
	};
*/
