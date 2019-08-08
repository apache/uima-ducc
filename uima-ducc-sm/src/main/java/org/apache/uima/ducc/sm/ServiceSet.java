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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import javax.naming.ServiceUnavailableException;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccSchedulerClasses;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccCompletionType.JobCompletionType;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceClass;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceState;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceType;
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

	private DuccLogger logger = DuccLogger.getLogger(this.getClass().getName(), COMPONENT_NAME);	
    private ServiceHandler handler;
    private IStateServices stateHandler;

    // key is unique id of descriptor.  The descriptor inherits key from a Job's DuccId, or from
    // a unique-to SM key for implicit references.

    Map<Long, ServiceInstance> implementors = new HashMap<Long, ServiceInstance>();
    TreeMap<Integer, Integer>  available_instance_ids = new TreeMap<Integer, Integer>();  // UIMA-4258
    Map<Long, Integer>         pending_instances = new HashMap<Long, Integer>();          // For hot bounce, restore the instance ids UIMA-4258

    // List<ServiceInstance> pendingStarts = new LinkedList<ServiceInstance>();           // UIMA-4258 not used anywhere

    // key is job/service id, value is same.  it's a map for fast existence check
    Map<DuccId, DuccId> references = new HashMap<DuccId, DuccId>();

    // For a registered service, here is my registered id
    DuccId id;
    // UIMA-5244 HashMap<Long, DuccId> friendly_ids = new HashMap<Long, DuccId>();
    String history_key = IStateServices.SvcMetaProps.work_instances.pname();
    String implementors_key = IStateServices.SvcMetaProps.implementors.pname();

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

    // Automatically start at boot, and keep implementors alive
    boolean autostart = false;
    // We've been stopped, which is used to override autostart
    // boolean stopped   = false;     // TODO Must get rid of this entirely
    boolean enabled   = true;

    // We've been started, so we know to enforce instance count even if not autostarted
    boolean started   = false;
    // Remember if was started by reference only so we can stop when refs die
    boolean reference_start = false;
    // is it ping-only?
    boolean ping_only = false;
    // debug specified in the registration?
    boolean process_debug = false;

    // Date of last known use of the service.  0 means "I don't know"
    long last_use = 0;

    // Date of last known successful ping of the service.  0 means never.  UIMA-4309
    long last_ping = 0;

    boolean notPinging = false;
    String notPingingReason = null;
    
    // Date of last known time any instance made it to Running state.  0 means never. UIMA-4309
    long last_runnable = 0;

    // The number of instances to maintain live.
    // Pinger or manual start/stop may make the number of live instances differ from the registered number
    int instances = 1;
    int registered_instances;

    // Service monitor / pinger 
    IServiceMeta serviceMeta = null;

    // registered services state files
    private DuccProperties job_props  = null;
    private DuccProperties meta_props = null;

    boolean deregistered = false;

    ServiceType  service_type  = ServiceType.Undefined;
    ServiceClass service_class = ServiceClass.Undefined;
    ServiceState service_state = ServiceState.Stopped;;

    // structures to manage service linger after it exits
    Timer timer = null;
    LingerTask linger = null;
    long linger_time = 60000;

    int init_failure_max = ServiceManagerComponent.init_failure_max;
    int init_failures = 0;                   // max allowed consecutive failures, current failure count
    
    int ping_failure_max = ServiceManagerComponent.failure_max;
    int ping_failures = 0;                   // for ping-only services, if the external pinger throws errors we
                                             // need to govern it

    int run_failures = 0;
    boolean excessiveRunFailures = false;       // signaled by monitor / pinger if we have too many

    boolean inShutdown = false;

    String[] coOwners = null;

    //  Swapped these 2 values  UIMA-5244
    String archive_key = IStateServices.SvcMetaProps.is_archived.columnName();
    String archive_flag  = "true";
    
    private String warning = "";  // May hold class-change msg

    //
    // Constructor for a registered service
    //
    public ServiceSet(ServiceHandler handler, IStateServices stateHandler, DuccId id, DuccProperties props, DuccProperties meta)
    {
        this.handler = handler;
        this.stateHandler = stateHandler;
        this.job_props = props;
        this.meta_props = meta;
        this.id = id;

        // Check for valid scheduling class here (was in the CLI)
        validateSchedulingClass(props);
        
        this.service_state = ServiceState.Stopped;
        this.linger_time = props.getLongProperty(UiOption.ServiceLinger.pname(), linger_time);
        this.key = meta.getProperty(IStateServices.SvcMetaProps.endpoint.pname());

        parseEndpoint(key);

        this.user = meta.getProperty(IStateServices.SvcMetaProps.user.pname());
        this.instances = meta.getIntProperty(IStateServices.SvcMetaProps.instances.pname(), 1);
        this.registered_instances = this.instances;
        this.autostart = meta.getBooleanProperty(IStateServices.SvcMetaProps.autostart.pname(), false);
        this.ping_only = meta.getBooleanProperty(IStateServices.SvcMetaProps.ping_only.pname(), false);
        this.enabled   = meta.getBooleanProperty(IStateServices.SvcMetaProps.enabled.pname(), enabled);
        this.service_class = ServiceClass.Registered;
        this.init_failure_max = props.getIntProperty(IStateServices.SvcRegProps.instance_init_failures_limit.pname(), init_failure_max);
        this.reference_start = meta.getBooleanProperty(IStateServices.SvcMetaProps.reference.pname(), this.reference_start);

        
        // Check if key has a value vs. empty or missing
        if ( props.containsKey(UiOption.ProcessDebug.pname()) &&
                ((String) props.get(UiOption.ProcessDebug.pname())).length() > 0)  {
            this.process_debug = true;
        }

        if ( props.containsKey(UiOption.Administrators.pname()) ) {
            String adm = props.getProperty(UiOption.Administrators.pname());
            if ( adm != null ) {
                coOwners = adm.split("\\s+");
            }
        }

        parseIndependentServices();

        meta_props.put(IStateServices.SvcMetaProps.references.pname(), "");         // Will get refreshed in upcoming OR state messages
        meta_props.remove(IStateServices.SvcMetaProps.stopped.pname());             // obsolete flag, clean out of older registrations

        meta_props.put(IStateServices.SvcMetaProps.service_class.pname(), ""+service_class.decode());
        meta_props.put(IStateServices.SvcMetaProps.service_type.pname(), ""+service_type.decode());
        meta_props.put(IStateServices.SvcMetaProps.enabled.pname(), "" + enabled);         // may not have been there in the first place
        meta_props.put(IStateServices.SvcMetaProps.service_state.pname(), ""+getState());
        meta_props.put(IStateServices.SvcMetaProps.ping_active.pname(), "false");
        meta_props.put(IStateServices.SvcMetaProps.service_alive.pname(),      "false");
        meta_props.put(IStateServices.SvcMetaProps.service_healthy.pname(),    "false");
        meta_props.put(IStateServices.SvcMetaProps.service_statistics.pname(), "N/A");
        setReferenced(this.reference_start);

        setLastUse(meta_props.getLongProperty(IStateServices.SvcMetaProps.last_use.pname(), 0L));
        setLastPing(meta_props.getLongProperty(IStateServices.SvcMetaProps.last_ping.pname(), 0L));
        setLastRunnable(meta_props.getLongProperty(IStateServices.SvcMetaProps.last_runnable.pname(), 0L));

        if ( (!job_props.containsKey(UiOption.ProcessExecutable.pname())) && (service_type != ServiceType.UimaAs) ) {
            meta_props.put(IStateServices.SvcMetaProps.ping_only.pname(), "true");
            this.ping_only = true;
        } else {
            meta_props.put(IStateServices.SvcMetaProps.ping_only.pname(), "false");
            this.ping_only = false;
        }

        savePendingInstanceIds();         // UIMA-4258
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

    String getEndpoint() {
    	return key;
    }
    
    String getWarning() {
      return warning;
    }
    
    // UIMA-4258
    // Get potentially pending instances from meta and stash them away for a bit
    // Used in hot-start to remap instance ids to ducc ids
    void savePendingInstanceIds()
    {
        String ids = meta_props.getProperty(implementors_key);
        if ( ids == null ) return;

        // UIMA-4258 Conversion: if no . then there is no instance, and it is an old service format service.
        //           Must remove the implementors from the meta and return.
        //
        if ( ids.indexOf(".") <= 0 ) {
            meta_props.put(implementors_key, "");
            return;
        }

        String[] tmp = ids.split("\\s+");
        for (String s : tmp) {
            String[] id_inst = s.split("\\.");
            pending_instances.put(Long.parseLong(id_inst[0]), Integer.parseInt(id_inst[1]));
        }
    }

    void parseEndpoint(String ep)
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

    // Check if a valid class name & if must be changed to a fixed one
    private void validateSchedulingClass(DuccProperties properties) {
      DuccSchedulerClasses duccSchedulerClasses = DuccSchedulerClasses.getInstance();
      String key = UiOption.SchedulingClass.pname();
      String schedulingClass = properties.getProperty(key);
      try {
        if (schedulingClass == null) {
          properties.setProperty(key, duccSchedulerClasses.getDebugClassDefaultName());
          return;
        }
        //if (!duccSchedulerClasses.getClasses().containsKey(schedulingClass)) {
        //  throw new IllegalArgumentException("Unknown scheduling_class: " + schedulingClass);
        //}
        if (duccSchedulerClasses.isPreemptable(schedulingClass)) {
          String fixedClass = duccSchedulerClasses.getDebugClassSpecificName(schedulingClass);
          if (fixedClass == null) {
            throw new IllegalArgumentException("Invalid class configuration - all classes must have a debug (fixed) entry");
          }
          properties.setProperty(key, fixedClass);
          warning = " (changed preemptable " + key + " to " + fixedClass + ")";
        }
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
    }

    synchronized Long[] getImplementors()
    {
        return implementors.keySet().toArray(new Long[implementors.size()]);
    }

    synchronized String getHostFor(Long implid)
    {
        return implementors.get(implid).getHost();
    }

    synchronized long getShareFor(Long implid)
    {
        return implementors.get(implid).getShareId();
    }

    synchronized DuccId[] getReferences()
    {
        return references.keySet().toArray(new DuccId[references.size()]);
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

    void deleteJobProperty(String k) {
        job_props.put(k, "");
    }

    void setJobProperty(String k, String v)
    {
        job_props.put(k, v);
    }

    void setMetaProperty(String k, String v)
    {
        meta_props.put(k, v);
    }

    boolean isDebug()
    {
        return process_debug;
    }

    /**
     * Is 'user' a registered co-owner?
     */
    boolean isAuthorized(String user)
    {
        if ( coOwners == null ) return false;
        for ( String s : coOwners ) {
            if ( s.equals(user) ) return true;
        }
        return false;
    }

    void parseAdministrators(String admins)
    {
        if ( admins != null ) {
            coOwners = admins.split("\\s+");
        }
    }

    private void parseIndependentServices()
    {
        String depstr = job_props.getProperty(UiOption.ServiceDependency.pname());
        String[] result = null;

        if ( depstr != null ) {
            result = depstr.split("\\s+");
            for ( int i = 0; i < result.length; i++ ) {
                result[i] = result[i].trim();
            }
        }
        independentServices = result;
    }

    /**
     * At boot only ... synchronize my state with published OR state.
     * 
     * We do this in the first phase of boot, then bootComplete is called to synchronize
     * history and update the physical meta properties file.
     */
    Map<Long, ServiceInstance> pendingImplementors = new HashMap<Long, ServiceInstance>();
    void bootImplementor(DuccId id, JobState state)
    {
    	String methodName = "bootImplementor";
        ServiceInstance si = new ServiceInstance(this);

        if ( ! pending_instances.containsKey(id.getFriendly()) ) {
            logger.warn(methodName, id, "Incoming Orchestrator state indicates active service instance but it is not in my meta data.");
            logger.warn(methodName, id, "Instance ignored.  This is usally caused by system or database failure.");
            return;
        }

        si.setState(state);
        si.setId(id.getFriendly());
        si.setStopped(false);
        si.setUser(this.user);
        si.setInstanceId(pending_instances.get(id.getFriendly())); // UIMA-4258

        handler.addInstance(this, si);
        pendingImplementors.put(id.getFriendly(), si);        // remember which instances we hear about in current OR publication
    }

    /**
     * Second phase, update history, and physical metaprops.
     */
    void bootComplete()
        throws Exception
    {
        //String methodName = "bootComplete";
        //
        // During boot, inactive implementors are removed.  Here we cull the implementors list to
        // remove stuff that didn't come in.
        //

        if ( isPingOnly() && enabled() ) {
            start();   // nothing to recover but we need the pseudo service to run
            return;   
        }
        implementors = pendingImplementors;   // only the ones that check in.  others are toast

        // 
        // must update history against stuff we used to have and don't any more
        //
        // TODO: update the history record in the meta
        //

        // UIMA-4258 restore instance ID if this is a hot restart
        if ( pending_instances.size() != 0 ) {
            TreeMap<Integer, Integer> nst = new TreeMap<Integer, Integer>();
            for (int i : pending_instances.values()) {
                nst.put(i, i);
            }
            int ndx = 0;
            while ( nst.size() > 0 ) {
                if ( nst.containsKey(ndx) ) {
                    nst.remove(ndx);
                } else {
                    available_instance_ids.put(ndx, ndx);
                }
                ndx++;
            }
        }
        pending_instances = null;

        // on restart, if we think we were ref started when we crashed, but there are no
        // implementors, we can't actually be ref started, so clean that up.
        if ( isReferencedStart() && (countImplementors() == 0 ) ) {
            this.reference_start = false;
        }
        updateMetaProperties();
    }

    
    /**
     *
     */
    synchronized void enforceAutostart()
    {
        String methodName = "enforceAutostart";
        if ( ! autostart ) return;                           // not doing auto, nothing to do
        if ( ! enabled() ) return;                           // doing auto, but we are disabled
        if ( init_failures >= init_failure_max ) return;     // too many init failures, no more enforcement
        if ( ping_failures >= ping_failure_max ) return;     // not pinging, let's not start more stuff
        
        // could have more implementors than instances if some were started dynamically but the count not persisted via registration
        int needed = Math.max(0, instances - countImplementors());
        if ( needed > 0 ) {
            logger.info(methodName, id, "Autostarting", needed, "instance" + ((needed > 1) ? "s" : ""), "already have", countImplementors());
            start();
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

    DuccProperties getJobProperties()
    {
        return job_props;
    }

    DuccProperties getMetaProperties()
    {
        return meta_props;
    }

    boolean isPingOnly()
    {
        return ping_only;
    }

    synchronized long getLastUse()
    {
        return last_use;
    }

    // UIMA-4309
    synchronized long getLastPing()
    {
        return last_ping;
    }

    // UIMA-4309
    synchronized long getLastRunnable()
    {
        return last_runnable;
    }

    synchronized void setLastUse(long lu)
    {
        this.last_use = lu;
        meta_props.put(IStateServices.SvcMetaProps.last_use.pname(), Long.toString(lu));
        if ( last_use == 0 ) {
            meta_props.put(IStateServices.SvcMetaProps.last_use_readable.pname(), "Unknown");
        } else {
            meta_props.put(IStateServices.SvcMetaProps.last_use_readable.pname(), (new Date(lu)).toString());
        }
    }

    // UIMA-4309
    synchronized void setLastPing(long lp)
    {
        this.last_ping = lp;
        meta_props.put(IStateServices.SvcMetaProps.last_ping.pname(), Long.toString(lp));
        if ( last_ping == 0 ) {
            meta_props.put(IStateServices.SvcMetaProps.last_ping_readable.pname(), "Unknown");
        } else {
            meta_props.put(IStateServices.SvcMetaProps.last_ping_readable.pname(), (new Date(lp)).toString());
        }
    }

    // UIMA-4309
    synchronized void setLastRunnable(long lr)
    {
        this.last_runnable = lr;
        meta_props.put(IStateServices.SvcMetaProps.last_runnable.pname(), Long.toString(lr));
        if ( last_runnable == 0 ) {
            meta_props.put(IStateServices.SvcMetaProps.last_runnable_readable.pname(), "Unknown");
        } else {
            meta_props.put(IStateServices.SvcMetaProps.last_runnable_readable.pname(), (new Date(lr)).toString());
        }
    }

    synchronized void resetRuntimeErrors()
    {
        run_failures = 0;
        ping_failures = 0;
        init_failures = 0;
        // Can't just remove as DB is updated from entries in the map
        meta_props.put(IStateServices.SvcMetaProps.submit_error.pname(), "");
        excessiveRunFailures = false;
    }

    synchronized void setAutostart(boolean auto)
    {
        if (!this.autostart && auto) {   // UIMA-5390 Restrict these resets to only if autostart was off but is now on 
            // turning this on gives benefit of the doubt on failure management
            // by definition, an autostarted services is NOT reference started
            cancelLinger();
            setReferenced(false);
            init_failures = 0;
            resetRuntimeErrors();
        }
        meta_props.setProperty(IStateServices.SvcMetaProps.autostart.pname(), auto ? "true" : "false");
        this.autostart = auto;
    }

    synchronized void restartPinger()
    {
        stopPingThread(); 
        resetRuntimeErrors();
    }

    /**
     * Manual start: turn off manual stop
     *               override reference_start
     *               remember manual start was done
     */
    synchronized void setStarted()
    {
        started = true;
        init_failures = 0;
    }

    /**
     * Manual stop: override reference_start and manual start.
     *              remember 'stopped' so enforceAutostart doesn't restart
     */
    // synchronized void setStopped()
    // {
    //     started = false;
    //     stopped = true;
    // }

    /**
     * Start by reference: if autostarted or already manually started, don't change anything
     *                     else remember we're ref started and not stopped
     */
    // synchronized void xsetReferencedStart(boolean is_start)
    // {
    //     if ( is_start ) {
    //         if ( isAutostart() || isStarted() ) return;
    //         this.stopped = false;
    //         this.reference_start = true;
    //         init_failures = 0;
    //         resetRuntimeErrors();
    //     } else {
    //         this.reference_start = false;
    //     }
    // }

    /**
     * Is the service stopped or about to stop?
     */
    synchronized boolean isStopped()
    {
        switch ( service_state ) {
            case Stopping:
            case Stopped:
                return true;
            default:
                return false;
        }
    }

    synchronized void ignoreReferences()
    {
        setReferenced(false);
        cancelLinger();
    }

    synchronized void observeReferences()
    {
        setReferenced(true);
        if ( countReferences() == 0 ) {
            lingeringStop();
        }
    }

    synchronized void disable(String reason)
    {
        meta_props.put(IStateServices.SvcMetaProps.disable_reason.pname(), reason);
        this.enabled = false;
    }

    synchronized void enable()
    {
        // Can't just remove as DB is updated from entries in the map
        meta_props.put(IStateServices.SvcMetaProps.disable_reason.pname(), "");
        resetRuntimeErrors();
        this.enabled = true;
    }

    synchronized boolean enabled()
    {
        return this.enabled;
    }

    synchronized String getDisableReason()
    {
        return meta_props.getStringProperty(IStateServices.SvcMetaProps.disable_reason.pname(), "Unknown");
    }

    /**
     * "Manually" started.
     */
    synchronized boolean isStarted()
    {
        return this.started;
    }

    /**
     * Started "by reference"
     */
    synchronized boolean isReferencedStart()
    {
        return this.reference_start;
    }

    synchronized boolean isAutostart()
    {
        return this.autostart;
    }

    String getUser()
    {
        return user;
    }

    boolean isDeregistered()
    {
        return deregistered;
    }

    void deregister()
    {
        deregistered = true;
    }

    // /**
    //  * Returns the number of currently running instances
    //  */
    // synchronized int getNInstances()
    // {
    //     return instances;
    // }

    /**
     * Returns the number of registered instances.
     */
    synchronized int getNInstancesRegistered()
    {
        return registered_instances;
    }

    /**
     * Service is unregistered, remove props from main DB, write them into the history DB.
     */
    synchronized void deleteProperties()
        throws Exception
    {
    	String methodName = "deleteProperties";

        String ak = meta_props.getProperty(archive_key);
        if ( (ak != null ) && ak.equals(archive_flag) ) { // (Migth not be set, that's ok)
            // Because of races and stuff we can get called more than once but should only
            //  archive once.
            logger.info(methodName, id, "Bypassing move to history; already moved.");
            return;
        }

        /*   UIMA-5244 Can remove this as friendly_ids is always empty
        String history = meta_props.getStringProperty(history_key, "");
        for ( Long id : friendly_ids.keySet() ) {
            history = history + " " + id.toString();
        }
        meta_props.put(history_key, history);
        */
        meta_props.put(archive_key, archive_flag);

        try {
            stateHandler.moveToHistory(id, job_props, meta_props);
        } catch ( Exception e ) {
            logger.error(methodName, id, "Could not move properties files to history: ", e);
        }
    }


    /**
     * Save both properties in a single transaction.
     */
    synchronized void storeProperties(boolean isRecovered)
        throws Exception
    {
    	//String methodName = "storeProperties";

        // no, don't store if it gets deregistered this fast
        if ( isDeregistered() ) return;

        //Long strid = id.getFriendly();
        prepareMetaProperties();     // these always need house cleaning before storing or syncing

        if ( ! isRecovered ) {       // if not recovery, no need to mess with the record
            stateHandler.storeProperties(id, job_props, meta_props);
        } else {                
            stateHandler.updateJobProperties(id, (Properties) job_props);
            stateHandler.updateMetaProperties(id, meta_props);
        }
    }

    synchronized void updateSvcProperties()
        throws Exception
    {
        // no more changes
        if ( isDeregistered() ) return;

        stateHandler.updateJobProperties(id, (Properties) job_props);
    }

    synchronized void updateMetaProperties()
    	throws Exception
    {
    	// String methodName = "saveMetaProperties";
        if ( isDeregistered() ) return;                // we may have deleted the properties but stuff
                                                       // lingers out of our control.  no more updates
                                                       // which can leave junk in the registry directory
                                                       // for file-based registry.

        prepareMetaProperties();
        stateHandler.updateMetaProperties(id, meta_props);
    }
    
    private long pingStabilityDefault = 10;
    private long pingRateDefault = 60 * 1000;
    
    private long pingStability = -1;
	private long pingRate = -1;
    
	private void configPingStability() {
		pingStability = SystemPropertyResolver.getLongProperty("ducc.sm.meta.ping.stability", pingStabilityDefault);
	}
	
	private void configPingRate() {
		pingRate = SystemPropertyResolver.getLongProperty("ducc.sm.meta.ping.rate", pingRateDefault);
	}
	
	private void configPing() {
		configPingRate();
		configPingStability();
	}
	
	/**
	 * If the Service state is Available but the pinger data has not been updated
	 * beyond the expiry time then determine that the pinger data is stale
	 * 
	 * The expiry time is calculated as pingStability * pingRate, nominally 10 * 60000.
	 */
    private void determinePingerStatus() {
    	String location = "determinePingerStatus";
    	switch(getState()) {
    	case Available:
    	case Waiting:
    		if(serviceMeta == null) {
    			notPinging = true;
    			notPingingReason = "pinger is not running";
    		}
    		else {
    			configPing();
    			long pingExpiry = pingStability * pingRate;
        		long now = System.currentTimeMillis();
        		long pingElapsed = now - last_ping;
        		if (pingElapsed > pingExpiry && last_ping != 0) {    // Don't treat first ping as stale
        			notPinging = true;
        			notPingingReason = "pinger data is stale";
        		}
        		else {
        			notPinging = false;
        			notPingingReason = "N/A";
        		}
        	}
    		break;
    	default:
    		notPinging = false;
			notPingingReason = "N/A";
    		break;
    	}
    	if(notPinging) {
    		logger.info(location, id, notPingingReason);
    		switch(service_state) {
    		case Available:
    			logger.info(location, id, service_state+" => "+ServiceState.Waiting);
    			service_state = ServiceState.Waiting;
    		}
    	}
    	return;
    }
    
    void prepareMetaProperties()
    {
        // String methodName = "saveMetaProperties";
        
        // try {
        //     throw new IllegalStateException("Saving meta properties");
        // } catch ( Throwable t) {
        //     t.printStackTrace();
        // }
        
        // UIMA-4587 Why bypass, as state can still dribble in.
        // if ( isDeregistered() ) return;

        if ( implementors.size() == 0 ) {
            meta_props.put(implementors_key, "");
        } else {
            StringBuffer sb_ducc_id = new StringBuffer();
            for ( Long l : implementors.keySet() ) {
                // UIMA-4258 Add instance id to ducc id when saving
                ServiceInstance inst = implementors.get(l);
                sb_ducc_id.append(Long.toString(l));
                sb_ducc_id.append(".");
                sb_ducc_id.append(Integer.toString(inst.getInstanceId()));
                sb_ducc_id.append(" ");
            }
            String s = sb_ducc_id.toString().trim();
            meta_props.setProperty(implementors_key, s);
        }

        determinePingerStatus();
        
        meta_props.put(IStateServices.SvcMetaProps.reference.pname(), isReferencedStart() ? "true" : "false");
        meta_props.put(IStateServices.SvcMetaProps.autostart.pname(), isAutostart()       ? "true" : "false");

        meta_props.put(IStateServices.SvcMetaProps.enabled.pname(), ""+enabled);
        meta_props.put(IStateServices.SvcMetaProps.service_state.pname(), ""+ getState());
        meta_props.put(IStateServices.SvcMetaProps.ping_active.pname(), "" + !notPinging);
        meta_props.put(IStateServices.SvcMetaProps.service_alive.pname(),      "false");
        meta_props.put(IStateServices.SvcMetaProps.service_healthy.pname(),    "false");

        if ( excessiveFailures() ) {
        	String msg = init_failures >= init_failure_max ? "initialization failures [" + init_failures + "]" 
        			                                       : "runtime failures [" + run_failures + "]";
        	meta_props.put(IStateServices.SvcMetaProps.submit_error.pname(), "Service disabled by excessive " + msg);
        } else {
            meta_props.put(IStateServices.SvcMetaProps.service_statistics.pname(), "N/A");
        }
        
        if ( serviceMeta != null ) {
            IServiceStatistics ss = serviceMeta.getServiceStatistics();
            if ( ss != null ) {
                meta_props.put(IStateServices.SvcMetaProps.service_alive.pname(),      "" + ss.isAlive());
                meta_props.put(IStateServices.SvcMetaProps.service_healthy.pname(),    "" + ss.isHealthy());
                meta_props.put(IStateServices.SvcMetaProps.service_statistics.pname(), "" + ss.getInfo());

                if ( ss.isAlive() ) {                    // UIMA-4309
                    setLastPing(serviceMeta.getServiceStatisticsTimestamp());
                }
            }
        }
        
        return;
    }

    synchronized void updateInstance(long iid, long share_id, String host)
    {
    	String methodName = "updateInstance";
        ServiceInstance inst = implementors.get(iid);
        if ( inst == null ) {
            logger.warn(methodName, id, "Cannot find instance", iid, "for update:", host + ":" + share_id);
            return;
        }
        inst.update(share_id, host);
    }

    /**
     * 
     * @param n     the new value for the register instance count
     * 
     * Called when the registration is updated via the CLI
     * Does NOT update the desired number of running instances as this can be set by the pinger.
     * Also modifications to the registration should not affect running instances.
     */
    synchronized void updateRegisteredInstances(int n)
    {
        meta_props.setProperty(IStateServices.SvcMetaProps.instances.pname(), Integer.toString(n));
        registered_instances = n;
    }

    /**
     * @param n      is the target number of instances we want running
     * 
     * This param dropped??
     * @param update indicates whether to match registration to the target
     * 
     * Called by doStart & doStop so may be making the running instances differ from the registered number
     */
    synchronized void updateInstances(int n)
    {
        if ( n >= 0 ) {
     
            instances = n;
            
            int running    = countImplementors();
            int diff       = n - running;
                
            if ( diff > 0 ) {
                start();
            } else if ( diff < 0 ) {
                stop(-diff); // TODO: no good, fix when changeTo is ready
            }
        }
    }

    synchronized void updateDebug(String val)
    {
        if ( val.equals("off") ) {
            job_props.put(UiOption.ProcessDebug.pname(), "");
            this.process_debug = false;
        } else {
            job_props.put(UiOption.ProcessDebug.pname(), val);
            this.process_debug = true;
        }
    }

    synchronized void updateLinger(String val)
    {
    	String methodName = "updateLinger";
        try {
            this.linger_time = Long.parseLong(val);
        } catch( NumberFormatException e ) {
            logger.error(methodName, id, "Cannot update linger, not numeric:", val);
        }
    }

    synchronized void updateInitFailureLimit(String val)
    {
    	String methodName = "updateInitFailureLimit";
        try {
            this.init_failure_max = Integer.parseInt(val);
        } catch( NumberFormatException e ) {
            logger.error(methodName, id, "Cannot update init failure max, not numeric:", val);
        }
    }


    synchronized void persistReferences()
    {
        String methodName = "persistReferences";

        if ( references.size() == 0 ) {
            meta_props.put(IStateServices.SvcMetaProps.references.pname(), "");
        } else {
            StringBuffer sb = new StringBuffer();
            for ( DuccId id : references.keySet() ) {
                sb.append(id.toString());
                sb.append(" ");
            }
            String s = sb.toString().trim();
            meta_props.setProperty(IStateServices.SvcMetaProps.references.pname(), s);
        }
        try {
            updateMetaProperties();
        } catch ( Exception e ) {
            logger.warn(methodName, id, "Cannot update meta properties:", e);
        }
    }

    void clearQueue()
    {
    	String methodName = "clearQueue";
    	
        if ( !deregistered ) {
            logger.info(methodName, id, "Not clearing queue because service is still registered.");
            return;
        }

        if ( implementors.size() != 0 ) {
            logger.info(methodName, id, "Not clearing queue because", implementors.size(), "implementors are still alive (", key, ").");
            return;
        }

        handler.removeService(this);
        try {
			deleteProperties();
		} catch (Exception e1) {
			logger.error(methodName, id, "Cannot complete deferred delete of properties:", e1);
		}

        if ( service_type != ServiceType.UimaAs ) {
            logger.info(methodName, id, "Deleting unregistered service; not clearing queue because this is not a UIMA-AS service:", key);
            return;
        }

        if ( isPingOnly() ) {
            logger.info(methodName, id, "Deleting unregistered service; not clearing queue for ping-only service", key);
            return;
        }

        String pingclass = job_props.getStringProperty(UiOption.ServicePingClass.pname(), UimaAsPing.class.getName());
        if ( !pingclass.equals(UimaAsPing.class.getName()) ) {
            logger.info(methodName, id, "Deleting unregistered service: not clearing queue because not using the default UIMA-AS pinger:", pingclass, "(", key, ")");
            return;
        }

        // Only do this if using the default pinger.  It's the pinger's job otherwise.
        UimaAsServiceMonitor monitor = new UimaAsServiceMonitor(endpoint, broker_host, broker_jmx_port);
        logger.info(methodName, id, "Deleting unregistered service and clearing queues for", key, "at [" + broker_host + ":" + broker_jmx_port + "]");
        try {
            monitor.init(null);
            monitor.clearQueues();
            monitor.stop(	);
        } catch (IOException e) {
            Throwable t = e.getCause();
            // ServiceUnavailbleException means it's gone already and we're processing some sort of stale state
            // that's still floating around.
            if ( ! (t instanceof ServiceUnavailableException) ) {
                logger.info(methodName, id, e);
            }
        } catch (Throwable e) {
            logger.info(methodName, id, e.toString());
        }
    }

    public synchronized int countImplementors()
    {
        return implementors.size();
    }

    public synchronized int countReferences()
    {
        return references.size();
    }

    public synchronized Long[] getActiveInstances()
    {
        ArrayList<Long> instIds = new ArrayList<Long>();
        for ( ServiceInstance inst : implementors.values() ) {
            if ( inst.isRunning() ) {
                instIds.add(inst.getId());
            }
        }
        return instIds.toArray(new Long[instIds.size()]);
    }

    synchronized void cancelLinger()
    {
    	String methodName = "cancelLinger";
        if ( linger != null ) {
            logger.debug(methodName, this.id, " ---------------- Canceling linger task");
            linger.cancel();
            linger = null;
        }
    }

    public void setErrorString(String s)
    	throws Exception
    {
        meta_props.put(IStateServices.SvcMetaProps.submit_error.pname(), s);
        updateMetaProperties();
    }

    public String getErrorString()
    {
        return meta_props.getProperty(IStateServices.SvcMetaProps.submit_error.pname()); 
    }

    void setReferenced(boolean r)
    {
        this.reference_start = r;
        meta_props.put(IStateServices.SvcMetaProps.reference.pname(), Boolean.toString(this.reference_start));
    }

    public synchronized void reference(DuccId id)
    {
        String methodName = "reference";

        logger.info(methodName, this.id, "Reference start requested by ", id);

        if ( ! enabled() ) {
             logger.warn(methodName, this.id, "Not reference starting new service instances because service is disabled.");
             return;
        }

        if ( excessiveFailures() ) {
            logger.warn(methodName, this.id, "Reference start fails, excessive failures: init[" + init_failures + "], run[" + run_failures + "]");
            return;
        }

        cancelLinger();
        references.put(id, id);
        logger.info(methodName, this.id, " References job/service", id, "count[" + references.size() + "] implementors [" + implementors.size() + "]");

        boolean idle = true;
        for (ServiceInstance si : implementors.values() ) {     // see if anything is running
            logger.debug(methodName, this.id, "Implementor", si.getId(), "state:", si.getState());
            if ( si.isRunning() ) {                             // and if so, no need to start anything
                idle = false;
                break;
            }
        }

        // Nothing running, so we do referenced start.
        if ( idle ) {
            logger.info(methodName, this.id, "Reference starting new service instances.");
            init_failures = 0;
            resetRuntimeErrors();
            setReferenced(true);
            start();
        } 

        persistReferences();
    }

    public synchronized void dereference(DuccId id)
    {
        String methodName = "dereference";
        if ( references.remove(id) == null ) {
            logger.error(methodName, this.id, "Dereference job/service",  id,  "not found in map for", getKey());
            return;
        }

        // stop the pinger if no longer needed
        if ( (references.size() == 0) && isReferencedStart() ) {                         // nothing left
            lingeringStop();
        }

        logger.info(methodName, this.id, " Dereferences job/service", id, "count[" + references.size() + "]");
        persistReferences();
    }

//     public synchronized int countReferences()
//     {
//         // note that this could change as soon as you get it so don't count on it being correct
//         // this is intended only for messages that don't have to be too accurate
//         return references.size();
//     }

    boolean containsImplementor(DuccId id)
    {
        return implementors.containsKey(id.getFriendly());
    }
    
    /**
     * Called by the PingDriver to return ping/monitor results, and to act on the results.
     *
     * @param nadditions           This is the number of new instances to start.
     * @param deletions            These are the specific instances to stop.
     * @param ndeleteions          This is the number of instances to stop.  This may well be smaller than
     *                             the size of the 'deletions' array because PingDriver caps deletions to
     *                             prevent over-aggressive or buggy monitors from killing a service.
     * @param isExcessiveFailuress This is set to 'true' if the ping/monitor decides there have been
     *                             too many instance failures and SM should stop trying to restart them.
     */
    synchronized void signalRebalance(int nadditions, Long[] deletions, int ndeletions, boolean isExcessiveFailures)
    {
        String methodName = "signalRebalance";
        logger.info(methodName, id, 
                    "PING: Additions:", nadditions, 
                    "deletions:", ndeletions, 
                    "excessive failures:", isExcessiveFailures, 
                    "implementors", countImplementors(),
                    "references", countReferences()
                    );

        ping_failures = 0;   
        
        this.excessiveRunFailures = isExcessiveFailures;
        
        // Note that nadditions could == ndeletions.  This is ok, because the monitor may want
        // to 'reboot' an instance by killing a specific one and also starting up a new one.
        
        if ( nadditions > 0) {
            start();
        }
        
        for ( int i = 0; i < ndeletions; i++ ) {
            instances -= stop(deletions[i]); // stop() may return 0 or 1
        }
        // keep services with autostart=true or with active references 
        // running with at least one instance on a rebalance
        // UIMA-4995 May happen when pinger says both delete 1 and add 1
        if ((this.isAutostart() || (this.countReferences() > 0) ) && instances == 0) {
          instances = 1;
        }

        try {
            updateMetaProperties();
        } catch ( Exception e ) {
            logger.warn(methodName, id, "Cannot update meta properties:", e);
        }
    }

    /**
     * Based on the state of the dwj, can we delete this instance from the records?
     */
    boolean canDeleteInstance(DuccWorkJob dwj)
    {
        // These are the job states
		// Received,				// Job has been vetted, persisted, and assigned unique Id
		// WaitingForDriver,		// Process Manager is launching Job Driver
		// WaitingForServices,		// Service Manager is checking/starting services for Job
		// WaitingForResources,	// Scheduler is assigning resources to Job
        // Assigned                 // passed basic tests, dispatched, not yet started to initialize
		// Initializing,			// Process Agents are initializing pipelines
		// Running,				// At least one Process Agent has reported process initialization complete
		// Completing,				// Job processing is completing
		// Completed,				// Job processing is completed
		// Undefined				// None of the above

        switch ( dwj.getJobState() ) {
            case Completing:
            case Completed:
                return true;
            default:
                return false;
        }
    }

    /**
     * We want to be sure the most-recently started instance get resources before 
     * allowing a new start.
     */
    boolean needNextStart(JobState old, JobState current)
    {
        // UIMA-4587 & UIMA-5244
    	String methodName="needNextStart";
    	// Can't do this before we handle the OR publication of "defunct" instances that were running when DUCC was shutdown
    	// They should not be counted as errors when the SM restarts.
 /*       if ( isDeregistered() || !enabled() ) {
            logger.info(methodName, id, "Bypassing instance start because service is unregistered or disabled.");*/
        if ( isDeregistered() ) {
            logger.info(methodName, id, "Bypassing instance start because service is unregistered.");
            return false;
        }

        switch ( old ) {
            case Received:
            case WaitingForDriver:
            case WaitingForServices:
            case WaitingForResources:
                switch (current) {
                    case Assigned:
                    case Initializing:
                    case Running:
                        return true;
                    default:
                        break;
                }
            default:
                break;
        }
        return false;
    }

    void removeImplementor(ServiceInstance si)
    {
    	String methodName = "removeImplementor";
        logger.info(methodName, id, "Removing implementor", si.getId());
        implementors.remove(si.getId());
        // Note, we don't save the instance id because this is only for ping-only services that have no instid
    }

    /**
     * This is one of my service instances.  Update its state and maybe kick the
     * state machine as well.
     TODO: proof this carefully
     */
    synchronized void signalUpdate(DuccWorkJob dwj)
    {
    	String methodName = "signalUpdate";
        ServiceInstance inst = implementors.get(dwj.getDuccId().getFriendly());
        
        if ( inst == null ) {            // he's gone and we don't care any more
            logger.warn(methodName, id, "Process", dwj.getDuccId(), "is no longer an implementor.  Perhaps it exited earlier.");
            return; 
        }

        JobState old_state = inst.getState();
        JobState state = dwj.getJobState();
        DuccId inst_id = dwj.getDuccId();
        long fid = inst_id.getFriendly();

        if ( state == JobState.Running && old_state != JobState.Running ) {
            // running, and wasn't before, we can reset the error counter
            logger.info(methodName, id, "Resetting init error counter from", init_failures, "to 0 on transition from", old_state, "to", state);
            init_failures = 0;
        }

        boolean save_meta = false;

        if ( needNextStart(old_state, state) ) {
            // sequnced startup
            start();
        }

        if ( canDeleteInstance(dwj) ) {
            // State Completed or Completing
            JobCompletionType jct = dwj.getCompletionType();
            ServiceInstance stoppedInstance = null;
            
            logger.info(methodName, this.id, "Removing implementor", fid, "(", key, ") completion", jct);
            stoppedInstance = implementors.remove(fid);          // won't fail, was checked for on entry
            conditionally_stash_instance_id(stoppedInstance.getInstanceId()); // UIMA-4258
            
            // TODO: put history into a better place
            String history = meta_props.getStringProperty(history_key, "");
            history = history + " " + fid;
            meta_props.put(history_key, history);
            save_meta = true;

            logger.info(methodName, id, "Removing stopped instance",  inst_id, "from maps: state[", state, "] completion[", jct, "] service-enabled", enabled());

            clearQueue();        // this won't do anything if it looks like the service is still active somehow

            if ( instances > countImplementors() ) {          // have we fallen short of the nInstances we have to maintain?
                
                // You can stop an instance with the ducc_services CLI, in which case this counts as a manual stop and not
                // an error.  Or the thing can go away for no clear reason, in which case it does as an error, even if somebody
                 // use the DuccServiceCancel API to stop it.
                //
                // TODO: Update the ducc_services CLI to allow stop and restart of specific instances without counting failure.
                if ( stoppedInstance.isStopped() ) {
                    logger.info(methodName, id, "Instance", inst_id, "stopped by SM.  Not restarting.");
                } else {
                    // An instance stopped and we (SM) didn't ask it to - by definition this is failure no matter how it exits.
 
                  // If the RM purges the instance on a node failure the service should not be penalized UIMA-6111
                  if (jct == JobCompletionType.ResourcesUnavailable) {
                    logger.info(methodName, id, "Instance purged by RM (node died?) - prior state[", old_state, 
                            "] current state[", state, "] completion[", jct, "]");
                  } else {
                    switch ( old_state ) {
                        case WaitingForServices:
                        case WaitingForResources:
                        case Initializing:
                        case Assigned:
                            init_failures++;
                            logger.info(methodName, id, "Tally initialization failure:", init_failures);
                            break;
                        case Running:
                            run_failures++;
                            logger.info(methodName, id, "Tally runtime failure", run_failures);
                            break;
                        default:
                            // other states we blow off - we can enter this place a bunch of time a things wind down
                            logger.info(methodName, id, "Instance stopped unexpectedly: prior state[", old_state, " completion[", jct, "]");
                            break;
                    }
                  }

                    if ( excessiveFailures() ) { 
                        String disable_reason = null;
                        if ( excessiveRunFailures ) {
                            logger.warn(methodName, id, "Instance", inst_id, "Monitor signals excessive terminations. Not restarting.");
                            disable_reason = "Excessive runtime errors";
                        } else {
                            logger.warn(methodName, id, "Instance", inst_id,
                                        "Excessive initialization failures. Total failures[" + init_failures + "]",
                                        "allowed [" + init_failure_max + "], not restarting.");
                            disable_reason = "Excessive initialization errors";
                        }
                        disable(disable_reason);
                        save_meta = true;
                    } else {
                        logger.warn(methodName, id, "Instance", inst_id + ": Unsolicited termination, not yet excessive.  Restarting instance.");
                        start();
                        return;         // don't use termination to set state - start will signal the state machine
                    }
                }
            }
        } 

        try {
            if ( save_meta ) updateMetaProperties();
        } catch ( Exception e ) {
            logger.warn(methodName, id, "Error updating meta properties:", e);
        }
        inst.setState(state);            
        signal(inst);
    }

    private ServiceState translateJobState(JobState js)
    {        
        switch ( js ) {
		    case Received:				// Job has been vetted, persisted, and assigned unique Id
		    case WaitingForDriver:		// Process Manager is launching Job Driver
		    case WaitingForServices:	// Service Manager is checking/starting services for Job
		    case WaitingForResources:	// Scheduler is assigning resources to Job
            case Assigned:
                return ServiceState.Starting;
		    case Initializing:			// Process Agents are initializing pipelines
                return ServiceState.Initializing;
		    case Running:				// At least one Process Agent has reported process initialization complete
                return ServiceState.Available;
		    case Completing:			// Job processing is completing
                return ServiceState.Stopping;
		    case Completed:				// Job processing is completed
                return ServiceState.Stopped;
		    default:
                return ServiceState.NotAvailable;  // Should not ever get here. It's a noop if we do.
        }
    }

    /**
     * The MAX of the states of the implementors.
     *   case Available:    return 8;
     *   case Waiting:      return 7;
     *   case Initializing: return 6;
     *   case Starting:     return 5;
     *   case Stopping:     return 4;
     *   case Stopped:      return 3;
     *   case NotAvailable: return 2;
     *   case Undefined:    return 1;
     *   default:           return 0;
     */
    private ServiceState cumulativeJobState()
    {
    	String methodName = "cumulativeJobState";
        ServiceState response = ServiceState.Stopped;

        for ( ServiceInstance si : implementors.values() ) {
            JobState js = si.getState();
            ServiceState translated = translateJobState(js);
            if (  translated.ordinality() > response.ordinality() ) response = translated;
        }
                
        // If there is a pinger, and it isn't pinging, we must not advance beyond the pinger's state.
        // If there is no pinger, we may never advance beyond Waiting
        if ( serviceMeta == null ) {
            response = (response.ordinality() < ServiceState.Waiting.ordinality()) ? response : ServiceState.Waiting;
        } else if ( serviceMeta != null ) {
            logger.trace(methodName, id, "Cumulative before checking monitor/pinger:", response, ".  Monitor state:", serviceMeta.getServiceState());
            if ( serviceMeta.getServiceState().ordinality() <= response.ordinality() ) response = serviceMeta.getServiceState();
        }

        return response;
    }

    synchronized ServiceState getState()
    {
        return service_state;
    }

    synchronized void setState(ServiceState ss)
    {
    	String methodName = "setState";
    	logger.debug(methodName, id, service_state, "==>", ss);
        service_state = ss;
    }
    
    synchronized void setState(ServiceState req_new_state, ServiceState req_cumulative, ServiceInstance si)
    {
        String methodName = "setState";

        String tail = "";
        if ( si == null ) {
            tail = "none/none";            
        } else {
            tail = si.getId() + "/" + si.getState();
        }

        ServiceState prev = this.service_state;
        switch(prev) {
        case Dispossessed:
        	logger.debug(methodName, id, prev);
        	return;
        }
        ServiceState new_state = req_new_state;
        ServiceState cumulative = req_cumulative;
        
        /**
         * If pinger is stale and state is Available then force state to be Waiting
         */
        determinePingerStatus();
        if(notPinging) {
        	switch(new_state) {
        	case Available:
        	case Waiting:	
        		new_state = ServiceState.Waiting;
        		cumulative = new_state;
        		logger.debug(methodName, id, "NotPinging[1]: "+req_new_state+" => "+new_state+"; "+req_cumulative+" => "+cumulative);
        		break;
        	default:
        		switch(cumulative) {
        		case Available:
        			new_state = ServiceState.Waiting;
            		cumulative = new_state;
            		logger.debug(methodName, id, "NotPinging[2]: "+req_new_state+" => "+new_state+"; "+req_cumulative+" => "+cumulative);
        			break;
        		default:
        			logger.debug(methodName, id, "NotPinging[3]: "+req_new_state+" => "+new_state+"; "+req_cumulative+" => "+cumulative);
        			break;
        		}
        		break;
        	}
        }
        else {
        	logger.debug(methodName, id, "Pinging: "+req_new_state+" => "+new_state+"; "+req_cumulative+" => "+cumulative);
        }
        
        this.service_state = new_state;
        if ( prev != new_state ) {
            logger.info(methodName, id, "State update from[" + prev + "] to[" + new_state + "] via[" + cumulative + "] Inst[" + tail + "]" );
            try {
                updateMetaProperties();
            } catch ( Exception e ) {
                logger.warn(methodName, id, "Error updating meta properties:", e);
            }

        }

        // Execute actions that must always occur based on the new state
        // These are all idempotent actions, call them as often as you want and no harm.
        switch(new_state) {            
            case Available: 
                setLastRunnable(System.currentTimeMillis());
                startPingThread();
                break;
            case Initializing:
                break;
            case Starting:
                break;
            case Waiting:
                setLastRunnable(System.currentTimeMillis());
                startPingThread();
                break;
            case Stopping:
                stopPingThread();
                break;
            case Stopped:
                setReferenced(false);
                stopPingThread();
                break;
            default:
                setReferenced(false);
                stopPingThread();
                break;
        }
    }

    public synchronized void signal(ServiceInstance si)
    {
        String methodName = "signal";

        if ( true ) {
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
            logger.trace(methodName, id, "serviceState", getState(), "cumulativeState", cumulative);
            switch ( getState() ) {
                // If I'm brand new and something is initting then I can be too.  If something is
                // actually running then I can start a pinger which will set my state.

            	case Dispossessed:
            		return;
                case Available:
                    switch ( cumulative ) {
                        case Starting:
                            logger.warn(methodName, id, "STATE REGRESSION:", getState(), "->", cumulative); // can't do anything about it but complain
                            setState(ServiceState.Starting, cumulative, si);
                            break;

                        case Initializing:
                            // Not immediately clear what would cause this other than an error but let's not crash.
                            logger.warn(methodName, id, "STATE REGRESSION:", getState(), "->", cumulative); // can't do anything about it but complain
                            setState(ServiceState.Initializing, cumulative, si);
                            break;

                        case Available:
                            setState(ServiceState.Available, cumulative, si);
                            break;

                        case Stopping:
                            setState(ServiceState.Stopping, cumulative, si);
                            break;

                        case Stopped:
                            setState(ServiceState.Stopped, cumulative, si);
                            break;

                        case Waiting:
                            setState(ServiceState.Waiting, cumulative, si);
                            break;

                        default:
                            stopPingThread();
                            logger.warn(methodName, id, "ILLEGAL STATE TRANSITION:", getState(), "->", cumulative); 
                            break;

                    }

                    break;

                    // If I'm initting and now something is running we can start a pinger
                case Initializing:
                    switch ( cumulative ) { 
                        case Starting:
                            logger.warn(methodName, id, "STATE REGRESSION:", getState(), "->", cumulative); // can't do anything about it but complain
                            setState(ServiceState.Starting, cumulative, si);
                            break;

                        case Initializing:
                            setState(ServiceState.Initializing, cumulative, si);
                            break;

                        case Available:
                            logger.warn(methodName, id, "UNEXPECTED STATE TRANSITION:", getState(), "->", cumulative); 
                            setState(ServiceState.Waiting, cumulative, si);
                            break;

                        case Stopping:
                            setState(ServiceState.Stopping, cumulative, si);
                            break;

                        case Stopped:
                            setState(ServiceState.Stopped, cumulative, si);
                          break;

                        case Waiting:
                            setState(ServiceState.Waiting, cumulative, si);
                            break;

                        default:
                            logger.warn(methodName, id, "ILLEGAL STATE TRANSITION:", getState(), "->", cumulative); 
                            break;
                    }
                    break;

                    // If I'm initting and now something is running we can start a pinger
                case Starting:
                    switch ( cumulative ) { 
                        case Starting:
                            setState(ServiceState.Starting, cumulative, si);
                            break;

                        case Initializing:
                            setState(ServiceState.Initializing, cumulative, si);
                            break;

                        case Available:
                            setState(ServiceState.Waiting, cumulative, si);
                            break;

                        case Stopping:
                            logger.info(methodName, id, "RETRY RETRY RETRY prevents state regression from Initializing");
                            break;

                        case Stopped:
                            setState(ServiceState.Stopped, cumulative, si);
                          break;

                        case Waiting:
                            logger.warn(methodName, id, "ILLEGAL STATE TRANSITION:", getState(), "->", cumulative); 
                            break;

                    }
                    break;

                case Waiting:                    
                    switch ( cumulative ) {
                        case Starting:
                            logger.warn(methodName, id, "STATE REGRESSION:", getState(), "->", cumulative); // can't do anything about it but complain
                            setState(ServiceState.Starting, cumulative, si);
                            break;

                        case Initializing:
                            logger.warn(methodName, id, "STATE REGRESSION:", getState(), "->", cumulative); // can't do anything about it but complain
                            setState(ServiceState.Initializing, cumulative, si);
                            break;

                        case Available:
                            setState(ServiceState.Available, cumulative, si);
                            break;

                        case Stopping:
                            setState(ServiceState.Stopping, cumulative, si);
                            break;

                        case Stopped:   
                            setState(ServiceState.Stopped, cumulative, si);
                            break;

                        case Waiting:
                            setState(ServiceState.Waiting, cumulative, si);
                            break;

                        default:
                            logger.warn(methodName, id, "ILLEGAL STATE TRANSITION:", getState(), "->", cumulative); 

                            break;

                    }
                    break;

                case Stopping:
                    switch ( cumulative ) {

                        case Starting:
                            setState(ServiceState.Starting, cumulative, si);
                            break;

                        case Initializing:
                            setState(ServiceState.Initializing, cumulative, si);
                            break;


                        case Available:                
                            setState(ServiceState.Available, cumulative, si);
                            break;

                        case Stopped:                
                            setState(ServiceState.Stopped, cumulative, si);
                            break;

                        case Stopping:                
                            setState(ServiceState.Stopping, cumulative, si);
                            break;

                        default:
                            logger.warn(methodName, id, "ILLEGAL STATE TRANSITION:", getState(), "->", cumulative); 
                            break;
                    }
                    break;                    

                case Stopped:
                    // OK
                    // Every transition can happen here because of hot-start of SM
                    switch ( cumulative ) { 
                        case Starting:
                            setState(ServiceState.Starting, cumulative, si);
                            break;

                        case Initializing:
                            setState(ServiceState.Initializing, cumulative, si);
                            break;

                        case Available:
                            setState(ServiceState.Waiting, cumulative, si);
                            break;

                        case Waiting:
                            setState(ServiceState.Waiting, cumulative, si);
                            break;
                        case Stopped:
                            // Trailing OR publications cause this.  Just record it for the log.
                            setState(ServiceState.Stopped, cumulative, si);
                            break;

                        case Stopping:
                            setState(ServiceState.Stopping, cumulative, si);
                            logger.warn(methodName, id, "UNEXPECTED STATE:", getState(), "->", cumulative); 
                            break;

                        case NotAvailable:
                            // junk.  just ignore it
                            logger.warn(methodName, id, "UNEXPECTED STATE:", getState(), "->", cumulative); 
                            break;
                    }
                    break;

                case NotAvailable:
                case Undefined:
                    // OK
                    logger.warn(methodName, id, "Illiegal state", getState(), "Ignored.");
                    break;
            }
        }

    }

    synchronized String getKey()
    {
        return key;
    }

    synchronized int getRunFailures()
    {
        return run_failures;
    }

    /**
     * Analyze failures - either too  many init failures, or the pinger says too many run failures.
     */
    synchronized boolean excessiveFailures()
    {
        String methodName = "excessiveFailures";
        if ( init_failures >= init_failure_max ) {
            logger.trace(methodName, id, "INIT FAILURES EXCEEDED");
            return true;
        } 
        
        if ( excessiveRunFailures ) {
            logger.trace(methodName, id, "EXCESSIVE RUN FAILURES SIGNALLED FROM SERVICE MONITOR.");
            return true;
        }

        return false;
    }

    private void startPingThread()
    {
    	String methodName = "startPingThread";
        if ( serviceMeta != null ) return;         // don't start multiple times.
        if ( inShutdown ) return;              // in shutdown, don't restart

        if ( ping_failures > ping_failure_max ) {
            String msg = "Service stopped as pinger failed to start " + ping_failures + " times.";
            logger.warn(methodName, id, msg);
            meta_props.put(IStateServices.SvcMetaProps.submit_error.pname(), msg);
            disableAndStop(msg);
            return;
        }

        try {
            logger.info(methodName, id, "Starting service monitor.");
            serviceMeta = new PingDriver(this);
        } catch ( Throwable t ) {
            logger.error(methodName, id, "Cannot instantiate service pinger.", t);
            return;
        }

        //setState(ServiceState.Waiting);
        Thread t = new Thread(serviceMeta);
        t.setName("Pinger-" + id.getFriendly());
        t.start();
    }

    synchronized void pingExited(int rc, PingDriver which_meta)
    {
        String methodName = "pingExited";

        logger.info(methodName, id, "Service Monitor/Pinger exits, rc", rc);
        if ( which_meta == serviceMeta ) {
            serviceMeta = null;
        } // otherwise, it was already removed by some intrepid unit

        if ( rc != 0 ) {
            ++ping_failures;
            logger.warn(methodName, id, "Ping exited with failure, total failures:", ping_failures);

            if ( isPingOnly() && (ping_failures > ping_failure_max) ) {
                logger.warn(methodName, id, "Stopping ping-only service due to excessive falutes:", ping_failure_max);
                meta_props.put(IStateServices.SvcMetaProps.submit_error.pname(), "Stopping ping-only service due to excessive falutes: " + ping_failure_max);

                stop(-1L);        // must be -lL Long to get the right overload
                implementors.remove(-1L);
            } 
        }

    }

    synchronized void stopMonitor()
    {
        String methodName = "stopMonitor";
        logger.info(methodName, id, "Stopping pinger due to shutdown");
        inShutdown = true;
        stopPingThread();
    }

    public synchronized void stopPingThread()
    {
        String methodName = "stopPingThread";

        if ( serviceMeta != null ) {
            logger.info(methodName, id, "Stopping monitor/ping thread for", key);
            serviceMeta.stop();
            serviceMeta = null;
        }

        try {
            updateMetaProperties();
        } catch ( Exception e ) {
            logger.warn(methodName, id, "Error updating meta properties:", e);
        }
    }

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
        pb.redirectOutput(new File("/dev/null"));
        pb.redirectError(new File("/dev/null"));
        try {
            Process p = pb.start();
            int rc = p.waitFor();
            if ( rc != 0 ) {
                logger.warn(methodName, id, "Attempt to update user's service.err.log returns with rc ", rc);
            }
        } catch (Throwable t) {
            logger.warn(methodName, id, "Cannot update user's service.err.log:", t);
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
     * See if there is an instance ID to reuse - if so, we need the lowest one.  
     * If not, assign the next on in sequence.
     *
     * This maintains the property that all instances sequential from 0 to max are either 
     * already assigned, or on the available_instance_ids tree.  Thus, if it's not on the tree,
     * we can find the next one by taking the lenght of the implementors structure.
     *
     * The reason we need to remember this is because pingers are allowed to stop specific
     * instances.  As well, specific instances may croak.  We always want to restart with the
     * lowest available instance if we have to reuse ids.
     */
    synchronized int find_next_instance()
    {
        int ret = implementors.size();
        if ( available_instance_ids.size() > 0 ) {
            ret = available_instance_ids.firstKey();
            available_instance_ids.remove(ret);
        }
        return ret;
    }

    /**
     * Save the id for possible reuse.
     *
     * It's an error, albeit non-fatal, if the instance is already stashed.
     * Can happen if an instance dies while  being stopped explicitly
     * UIMA-4258
     */
    synchronized void stash_instance_id(int instid)
    {
        /* UIMA-5244 No need to warn as is expected.
    	String methodName = "stash_intance_id";
        if ( available_instance_ids.containsKey(instid) ) {
            try {
                // put a scary marker in the log
                throw new Exception("Duplicate instance id found: " + instid);
            } catch (Exception e) {
                logger.warn(methodName, id, e);
            }
            return;
        }
        */

        available_instance_ids.put(instid, instid);
    }

    /**
     * Save the id for possible reuse, if it hasn't already been saved.
     * This is called when we see a state change that indicates a service has exited.  Usually we
     * hope this is because it was stopped, in which case we already stashed the id.  But if it
     * crashed it may not be stashed yet, so we do it here.
     * UIMA-4258
     */
    synchronized void conditionally_stash_instance_id(int instid)
    {
        if ( available_instance_ids.containsKey(instid) ) {           // Is this necessary? Could simply let the put replace
            return;
        }
        stash_instance_id(instid);        
    }

    synchronized void start()
    {
    	String methodName = "start";

        // UIMA-4587 & UIMA-5244
    	// Can't do this yet
/*        if ( isDeregistered() || !enabled()) {
            logger.info(methodName, id, "Bypass start because service is unregistered or disabled.");*/
    	if ( isDeregistered()) {
    	    logger.info(methodName, id, "Bypass start because service is unregistered.");
            return;
        }

        if ( countImplementors() >= instances ) {
            return;
        }

        if ( isPingOnly() ) {
            if ( implementors.containsKey(-1l) ) {
                logger.info(methodName, id, "PING_ONLY: already started.");
                return;
            }

            ServiceInstance si = new PingOnlyServiceInstance(this);
            si.setId(-1L);
            si.setUser(this.user);
            implementors.put(-1l, si);
            handler.addInstance(this, si);
            si.start(null, null);
            signal(si);
        } else {

            if ( isDebug() ) {
                if ( countImplementors() > 0 ) {
                    logger.warn(methodName, id, "Ignoring start of additional instances because process_debug is set.");
                    return;         // only one, in debug
                }
            }

            ServiceInstance si = new ServiceInstance(this);
            si.setInstanceId(find_next_instance());
            long inst_ducc_id = -1L;
            logger.info(methodName, id, "Starting instance. Current count", countImplementors(), "needed", instances);
            if ( (inst_ducc_id = si.start(job_props, meta_props)) >= 0 ) {
                implementors.put(inst_ducc_id, si);
                handler.addInstance(this, si);
                signal(si);
                logger.info(methodName, id, "Instance[", countImplementors(), "] ducc_id ", inst_ducc_id);
            } else {
                logger.info(methodName, id, "Instance[", countImplementors(), "] ducc_id ", inst_ducc_id, "Failed to start.");
                disable("Cannot submit service process");
                signal(si);
            }
        }    

        try {
            updateMetaProperties();
        } catch ( Exception e ) {
            logger.warn(methodName, id, "Error updating meta properties:", e);
        }        
    }

    /**
     * Stop a specific instance.
     */
    synchronized int stop(Long iid)
    {
        String methodName = "stop(id)";

        logger.info(methodName, id, "Stopping specific instance", iid);

        ServiceInstance si = implementors.get(iid);
        if ( si == null ) {
            logger.warn(methodName, id, "Can't find instance", iid, ", perhaps it's already gone.");
            return 0;
        } else {
            si.stop();
            stash_instance_id(si.getInstanceId());         // UIMA-4258
            signal(si);
            return 1;
        }
    }

    /**
     * Stop 'count' services.
     */
    synchronized void stop(int count)
    {
        String methodName = "stop(count)";

        logger.info(methodName, id, "Stopping", count, "implementors");

        Long[] keys = implementors.keySet().toArray(new Long[implementors.size()]);
        Arrays.sort(keys);
        for ( int i = 0, j = keys.length-1; i < count; i++, j-- ) {
            Stopper s = new Stopper(implementors.get(keys[j]));
            new Thread(s).start();
        }
    }

    synchronized void stopAll()
    {
        stop(implementors.size());
    }

    /**
     * Make the thing stop and not restart.
     */
    synchronized void disableAndStop(String reason)
    {
        disable(reason);
        stopAll();
        // instances = 0;     // Is this needed to ensure no more are started? UIMA-5244
    }

    // /**
    //  * Stop everything
    //  */
    // synchronized void stop()
    // {
    //     // TODO
    //     // change state to Stopping and spawn stop threads for all implementors
    //     for ( ServiceInstance si : implementors.values() ) {
    //         Stopper s = new Stopper(si);
    //         new Thread(s).start();
    //     }
    // }

    private class LingerTask
        extends TimerTask
    {
        //ServiceSet sset;
        //LingerTask(ServiceSet sset)
        LingerTask()
        {        
            String methodName = "LingerTask.init";
            logger.debug(methodName, id, "Linger starts", linger_time);
            //this.sset = sset;
        }

        public void run()
        {
            String methodName = "LingerTask.run";
            logger.info(methodName, id, "Linger time reached ... stopping all instances.");
            // doesn't matter how its started i think, we have to set this flag off when we stop
            linger = null;
            setReferenced(false);
            stopAll();
        }
    }

    void lingeringStop()
    {
        if ( timer == null ) {
            timer = new Timer();
        }
        //linger = new LingerTask(this);
        linger = new LingerTask();
        timer.schedule(linger, linger_time);
    }

    IServiceDescription query()
    {
        IServiceDescription sd = new ServiceDescription();
        
        ArrayList<Long> impls = new ArrayList<Long>();
        ArrayList<Integer> instids = new ArrayList<Integer>();
        for ( Long id : implementors.keySet() ) {
            // UIMA-4258 Add instance id to ducc id when saving
            ServiceInstance inst = implementors.get(id);            
            impls.add(id);
            instids.add(inst.getInstanceId());
        }
        sd.setImplementors(impls, instids);

        ArrayList<Long> ref = new ArrayList<Long>();
        ref.clear();
        for ( DuccId id : references.keySet() ) {
            ref.add(id.getFriendly());
        }
        sd.setReferences(ref);

        sd.setInstances(getNInstancesRegistered());

        sd.setType(service_type);
        sd.setSubclass(service_class);
        sd.setEndpoint(endpoint);
        sd.setBroker(broker);
        sd.setServiceState(getState());
        sd.setActive(serviceMeta != null);
        sd.setEnabled(enabled());
        sd.setAutostart(isAutostart());
        sd.setLinger(linger_time);
        sd.setId(id.getFriendly());
        sd.setUser(user);
        sd.setDisableReason(meta_props.getStringProperty(IStateServices.SvcMetaProps.disable_reason.pname(), null));
        sd.setLastUse(last_use);
        sd.setLastPing(last_ping);            // UIMA-4309
        sd.setLastRunnable(last_runnable);    // UIMA-4309
        sd.setRegistrationDate(meta_props.getStringProperty(IStateServices.SvcMetaProps.registration_date.pname(), ""));
        sd.setReferenceStart(reference_start);
        sd.setErrorString(meta_props.getStringProperty(IStateServices.SvcMetaProps.submit_error.pname(), null));

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

    class Stopper
        implements Runnable
    {
        ServiceInstance si;
        Stopper(ServiceInstance si)
        {
            this.si = si;
        }

        public void run() {
            si.stop();
            stash_instance_id(si.getInstanceId());         // UIMA-4258
        }
    }


}

/**
    For reference

	public enum JobState {
		Received,				// Job has been vetted, persisted, and assigned unique Id
		WaitingForDriver,		// Process Manager is launching Job Driver
		WaitingForServices,		// Service Manager is checking/starting services for Job
		WaitingForResources,	// Scheduler is assigning resources to Job
        Assigned,               // Resources assgned, job not yet started.
		Initializing,			// Process Agents are initializing pipelines
		Running,				// At least one Process Agent has reported process initialization complete
		Completing,				// Job processing is completing
		Completed,				// Job processing is completed
		Undefined				// None of the above
	};
*/
