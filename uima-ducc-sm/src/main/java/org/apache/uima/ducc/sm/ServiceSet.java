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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.cli.UimaAsPing;
import org.apache.uima.ducc.cli.UimaAsServiceMonitor;
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.id.ADuccId;
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
    
    // key is unique id of descriptor.  The descriptor inherites key from a Job's DuccId, or from
    // a unique-to SM key for implicit references.

    Map<Long, ServiceInstance> implementors = new HashMap<Long, ServiceInstance>();
    List<ServiceInstance> pendingStarts = new LinkedList<ServiceInstance>();

    // key is job/service id, value is same.  it's a map for fast existence check
    Map<DuccId, DuccId> references = new HashMap<DuccId, DuccId>();

    // For a registered service, here is my registered id
    DuccId id;
    HashMap<Long, DuccId> friendly_ids = new HashMap<Long, DuccId>();
    String history_key = "work-instances";
    String implementors_key = "implementors";

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
    boolean stopped   = false;
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

    // The number of instances to maintain live.
    int instances = 1;
    int registered_instances;

    // Service monitor / pinger 
    IServiceMeta serviceMeta = null;

    // registered services state files
    private DuccProperties job_props  = null;
    String props_filename = null;
    String props_filename_temp = null;
    File props_file;
    File props_file_temp;

    private DuccProperties meta_props = null;

    String meta_filename = null;
    String meta_filename_temp = null;
    File meta_file;
    File meta_file_temp;
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
    boolean excessiveRunFailures = false;       // signalled by monitor / pinger if we have too many

    boolean inShutdown = false;

    String[] coOwners = null;
    //
    // Constructor for a registered service
    //
    public ServiceSet(ServiceHandler handler, DuccId id, String props_filename, String meta_filename, DuccProperties props, DuccProperties meta)
    {
        this.handler = handler;
        this.job_props = props;
        this.meta_props = meta;
        this.id = id;

        this.props_filename = props_filename;
        this.props_filename_temp = props_filename + ".tmp";
        this.props_file = new File(props_filename);
        this.props_file_temp = new File(props_filename_temp);        
        
        this.meta_filename = meta_filename;
        this.meta_filename_temp = meta_filename + ".tmp";
        this.meta_file = new File(meta_filename);
        this.meta_file_temp = new File(meta_filename_temp);

        this.service_state = ServiceState.Stopped;
        this.linger_time = props.getLongProperty(UiOption.ServiceLinger.pname(), linger_time);
        this.key = meta.getProperty("endpoint");

        parseEndpoint(key);

        this.user = meta.getProperty("user");
        this.instances = meta.getIntProperty("instances", 1);
        this.registered_instances = this.instances;
        this.autostart = meta.getBooleanProperty("autostart", false);
        this.ping_only = meta.getBooleanProperty("ping-only", false);
        this.stopped   = meta.getBooleanProperty("stopped", stopped);
        this.service_class = ServiceClass.Registered;
        this.init_failure_max = props.getIntProperty("instance_init_failures_limit", init_failure_max);
        
        
        if ( props.containsKey(UiOption.ProcessDebug.pname()) ) {
            this.process_debug = true;
        }

        if ( props.containsKey(UiOption.Administrators.pname()) ) {
            String adm = props.getProperty(UiOption.Administrators.pname());
            if ( adm != null ) {
                coOwners = adm.split("\\s+");
            }
        }

        parseIndependentServices();

        meta_props.remove("references");          // Will get refreshred in upcoming OR state messages
        meta_props.put("service-class", ""+service_class.decode());
        meta_props.put("service-type", ""+service_type.decode());
        meta_props.put("stopped", ""+stopped);
        meta_props.put("service-state", ""+getState());
        meta_props.put("ping-active", "false");
        meta_props.put("service-alive",      "false");
        meta_props.put("service-healthy",    "false");
        meta_props.put("service-statistics", "N/A");
        meta_props.remove("submit-error");

        last_use = meta_props.getLongProperty("last-use", 0L);
        if ( last_use == 0 ) {
            meta_props.put("last-use", "0");
            meta_props.put("last-use-readable", "Unknown");
        }

        if ( (!job_props.containsKey(UiOption.ProcessExecutable.pname())) && (service_type != ServiceType.UimaAs) ) {
            meta_props.put("ping-only", "true");
            this.ping_only = true;
        } else {
            meta_props.put("ping-only", "false");
            this.ping_only = false;
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
        meta_props.remove(implementors_key);

        ServiceManagerComponent.deleteProperties(id.toString(), meta_filename, meta_props, props_filename, job_props);
        meta_filename = null;
        props_filename = null;
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
        job_props.remove(k);
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
        ServiceInstance si = new ServiceInstance(this);

        si.setState(state);
        si.setId(id.getFriendly());
        si.setStopped(false);
        si.setUser(this.user);

        handler.addInstance(this, si);
        pendingImplementors.put(id.getFriendly(), si);
    }

    /**
     * Second phase, update history, and physical metaprops.
     */
    void bootComplete()
    {
        //
        // During boot, inactive implementors are removed.  Here we cull the implementors list to
        // remove stuff that didn't come in.
        //

        if ( isPingOnly() && ! stopped) {
            start(1);   // nothing to recover but we need the pseudo service to run
            return;   
        }
        implementors = pendingImplementors;   // only the ones that check in.  others are toast

        // 
        // must update history against stuff we used to have and don't any more
        //
        String old_impls = meta_props.getProperty(implementors_key);
        if ( old_impls != null ) {
            Map<String, String> ip = new HashMap<String, String>();
            String[]   keys = old_impls.split("\\s+");
            for ( String k : keys ) ip.put(k, k);

            String history = meta_props.getProperty(history_key);
            Map<String, String> hp = new HashMap<String, String>();
            if ( history != null ) {
                keys = history.split("\\s+");
                for ( String k : keys ) hp.put(k, k);
            }

            // here, bop through the things we used to know about, and if
            // it's missing from what checked in, it's history.
            for ( String k : ip.keySet() ) {
                Long iid = Long.parseLong(k);
                if ( ! implementors.containsKey(iid) ) {
                    hp.put(k, k);
                }
            }

            // now put the history string back into the meta props
            if ( hp.size() > 0 ) {
                StringBuffer sb = new StringBuffer();
                for (String s : hp.keySet() ) {
                    sb.append(s);
                    sb.append(" ");
                }
                meta_props.setProperty(history_key, sb.toString().trim());
            }
        }
        saveMetaProperties();
    }

    
    /**
     *
     */
    synchronized void enforceAutostart()
     {
         String methodName = "enforceAutostart";
         if ( ! autostart ) return;                           // not doing auto, nothing to do
         if ( stopped     ) return;                           // doing auto, but we've been manually stopped
         if ( init_failures >= init_failure_max ) return;     // too many init failures, no more enforcement
         if ( ping_failures >= ping_failure_max ) return;     // ping-only 

         // if ( (isPingOnly()) && (serviceMeta == null) ) {    // ping-only and monitor / pinger not alive
         //     logger.info(methodName, id, "Autostarting 1 ping-only instance.");

         //     start(1);                                       // ... then it needs to be started
         //     return;
         // }
         
         // could have more implementors than instances if some were started dynamically but the count not persisted
         int needed = Math.max(0, instances - countImplementors());
         if ( needed > 0 ) {
             logger.info(methodName, id, "Autostarting", needed, "instance" + ((needed > 1) ? "s" : ""), "already have", countImplementors());
             start(needed);
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

    long getLastUse()
    {
        return last_use;
    }

    synchronized void setLastUse(long lu)
    {
        this.last_use = lu;
        meta_props.put("last-use", Long.toString(lu));
        if ( last_use != 0 ) {
            meta_props.put("last-use-readable", (new Date(lu)).toString());
        }
    }

    synchronized void resetRuntimeErrors()
    {
        run_failures = 0;
        ping_failures = 0;
        init_failures = 0;
        meta_props.remove("submit-error");
        excessiveRunFailures = false;
    }

    synchronized void setAutostart(boolean auto)
    {
        meta_props.setProperty("autostart", auto ? "true" : "false");
        this.autostart = auto;
        if ( auto ) {
            // turning this on gives benefit of the doubt on failure management
            // by definition, an autostarted services is NOT reference started
            cancelLinger();
            reference_start = false;
            init_failures = 0;
            resetRuntimeErrors();
        }
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
        stopped = false;
        reference_start = false;
        started = true;
        init_failures = 0;
    }

    /**
     * Manual stop: override reference_start and manual start.
     *              remember 'stopped' so enforceAutostart doesn't restart
     */
    synchronized void setStopped()
    {
        reference_start = false;
        started = false;
        stopped = true;
    }

    /**
     * Start by reference: if autostarted or already manually started, don't change anything
     *                     else remember we're ref started and not stopped
     */
    synchronized void setReferencedStart(boolean is_start)
    {
        if ( is_start ) {
            if ( isAutostart() || isStarted() ) return;
            this.stopped = false;
            this.reference_start = true;
            init_failures = 0;
            resetRuntimeErrors();
        } else {
            this.reference_start = false;
        }
    }

    /**
     * "Manually" stopped.
     */
    synchronized boolean isStopped()
    {
        return this.stopped;
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

    String getMetaFilename()
    {
        return meta_filename;
    }

    String getPropsFilename()
    {
        return props_filename;
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

    private void saveProperties(DuccProperties props, File pfile, File pfile_tmp, String type)
    {
    	
    	String methodName = "saveMetaProperties";
        FileOutputStream fos = null;
        try {
            if ( (!pfile.exists()) || pfile.renameTo(pfile_tmp) ) {
                fos = new FileOutputStream(pfile);
                props.store(fos, type + " Descriptor");
            } else {
                logger.warn(methodName, id, "Cannot save", type, "properties, rename of", pfile, "to", pfile_tmp, "fails.");
                if ( (!pfile.exists()) && pfile_tmp.exists() ) {
                    if ( !pfile_tmp.renameTo(pfile) ) {
                        logger.error(methodName, id, "Cannot restore", pfile_tmp, "to", pfile, "after failed update.");
                    }
                }
            }
		} catch (FileNotFoundException e) {
            logger.warn(methodName, id, "Cannot save", type, "properties, file does not exist.");
		} catch (IOException e) {
            logger.warn(methodName, id, "I/O Error saving", type, "service properties:", e);
		} finally {
            try {
				if ( fos != null ) fos.close();
                pfile_tmp.delete();
			} catch (IOException e) {
                logger.error(methodName, id, "Cannot close", type, "properties:", e);
			}
        }
    }

    synchronized void saveMetaProperties()
    {
        String methodName = "saveMetaProperties";
        
        // try {
        //     throw new IllegalStateException("Saving meta properties");
        // } catch ( Throwable t) {
        //     t.printStackTrace();
        // }
        
        if ( isDeregistered() ) return;

        if ( meta_filename == null ) {
            // if this is null it was deleted and this is some kind of lingering thread updating, that
            // we don't really want any more
            logger.error(methodName, id, "Meta properties is deleted, bypassing attempt to save.");
            return;
        }

        if ( implementors.size() == 0 ) {
            meta_props.remove(implementors_key);
        } else {
            StringBuffer sb = new StringBuffer();
            for ( Long l : implementors.keySet() ) {
                sb.append(Long.toString(l));
                sb.append(" ");
            }
            String s = sb.toString().trim();
            meta_props.setProperty(implementors_key, s);
        }

        meta_props.put("stopped", ""+stopped);
        meta_props.put("service-state", ""+ getState());
        meta_props.put("ping-active", "" + (serviceMeta != null));
        meta_props.put("service-alive",      "false");
        meta_props.put("service-healthy",    "false");

        if ( excessiveFailures() ) {
            meta_props.put("submit-error", "Service stopped by exessive failures.  Initialization failures[" + init_failures + "], Runtime failures[" + run_failures + "]");
        } else {
            meta_props.put("service-statistics", "N/A");
        }
        
        if ( serviceMeta != null ) {
            IServiceStatistics ss = serviceMeta.getServiceStatistics();
            if ( ss != null ) {
                meta_props.put("service-alive",      "" + ss.isAlive());
                meta_props.put("service-healthy",    "" + ss.isHealthy());
                meta_props.put("service-statistics", "" + ss.getInfo());
            }
        }

        saveProperties(meta_props, meta_file, meta_file_temp, "Meta");
                
        return;
    }

    void saveServiceProperties()
    {
        saveProperties(job_props, props_file, props_file_temp, "Service");
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

    synchronized void updateRegisteredInstances(int n)
    {
        meta_props.setProperty("instances", Integer.toString(n));
        registered_instances = n;
    }

    /**
     * @param n      is the target number of instances we want running
     * @param update indicates whether tp match registration to the target
     */
    synchronized void updateInstances(int n, boolean update)
    {
        if ( n >= 0 ) {
     
            instances = n;
            if ( update ) {
                updateRegisteredInstances(n);
            }

            int running    = countImplementors();
            int diff       = n - running;
                
            if ( diff > 0 ) {
                start(diff);
            } else if ( diff < 0 ) {
                stop(-diff);
            }
        }
    }

    synchronized void updateDebug(String val)
    {
        if ( val.equals("off") ) {
            job_props.remove(UiOption.ProcessDebug.pname());
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
        deleteProperties();

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
        } catch (Throwable e) {
            // totally not a problem, just lost it
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

    public String getErrorString()
    {
        return meta_props.getProperty("submit-error"); 
    }

    public synchronized void reference(DuccId id)
    {
        String methodName = "reference";

        logger.info(methodName, this.id, "Reference start requested by ", id);
        if ( excessiveFailures() ) {
            logger.warn(methodName, this.id, "Reference start fails, excessive failures: init[" + init_failures + "], run[" + run_failures + "]");
            return;
        }

        cancelLinger();
        references.put(id, id);
        logger.info(methodName, this.id, " References job/service", id, "count[" + references.size() + "] implementors [" + implementors.size() + "]");

        persistReferences();

        for (ServiceInstance si : implementors.values() ) {     // see if anything is running
            logger.debug(methodName, this.id, "Implementor", si.getId(), "state:", si.getState());
            if ( si.isRunning() ) return;                      // and if so, no need to start anything
        }

        // Nothing running, so we do referenced start.

        if ( ! isStopped() ) {
            logger.info(methodName, this.id, "Reference starting new service instances.");
            setReferencedStart(true);
            start(registered_instances);
        } else {
            logger.info(methodName, this.id, "Not reference starting new service instances because service is stopped.");
        }

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
     *                             prevent over-agressive or buggy monitors from killing a service.
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

        while ( true ) {      
            // Kids, don't try this at home! 
            // All paths MUST lead to break or we loop forever - using while/break as goto mechanism
            ping_failures = 0;   
            
            this.excessiveRunFailures = isExcessiveFailures;

            // Note that nadditions could == ndeletions.  This is ok, because the monitor may want
            // to 'reboot' an instance by killing a specific one and also starting up a new one.

            if ( nadditions > 0) {
                start(nadditions);
            }
            
            for ( int i = 0; i < ndeletions; i++ ) {
                stop(deletions[i]);
            }

            break;   // required break
        }
        saveMetaProperties();
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
                
        if ( canDeleteInstance(dwj) ) {
            // State Completed or Completing
            JobCompletionType jct = dwj.getCompletionType();
            ServiceInstance stoppedInstance = null;
            
            logger.info(methodName, this.id, "Removing implementor", fid, "(", key, ") completion", jct);
            stoppedInstance = implementors.remove(fid);          // won't fail, was checked for on entry
            
            // TODO: put history into a better place
            String history = meta_props.getStringProperty(history_key, "");
            history = history + " " + fid;
            meta_props.put(history_key, history);
            saveMetaProperties();

            logger.info(methodName, id, "Removing stopped instance",  inst_id, "from maps: state[", state, "] completion[", jct, "] isStopped", isStopped());

            clearQueue();        // this won't do anything if it looks like the service is still active somehow

            if ( instances > countImplementors() ) {          // have we fallen short of the nInstances we have to maintain?
                
                // You can stop an instance with the ducc_services CLI, in which case this counts as a manual stop and not
                // an error.  Or the thing can go away for no clear reason, in which case it does as an error, even if somebody
                // use the DuccServiceCancel API to stop it.
                //
                // TODO: Update the ducc_services CLI to allow stop and restart of specific instances without counting failure.
                if ( stoppedInstance.isStopped() ) {
                    logger.info(methodName, id, "Instance", inst_id, "is manually stopped.  Not restarting.");
                } else {
                    // An instance stopped and we (SM) didn't ask it to - by definition this is failure no matter how it exits.
                    
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

                    if ( excessiveFailures() ) {
                        if ( excessiveRunFailures ) {
                            logger.warn(methodName, id, "Instance", inst_id, "Monitor signals excessive terminations. Not restarting.");
                        } else {
                            logger.warn(methodName, id, "Instance", inst_id,
                                        "Excessive initialization failures. Total failures[" + init_failures + "]",
                                        "allowed [" + init_failure_max + "], not restarting.");
                        }
                        setAutostart(false);
                    } else {
                        logger.warn(methodName, id, "Instance", inst_id + ": Uunsolicited termination, not yet excessive.  Restarting instance.");
                        start(1);
                        return;         // don't use termination to set state - start will signal the state machine
                    }
                }
            }
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
                
        // If there's no pinger we don't adjust, so the state machine can do magic to start one.
        // If there is a pinger, and it isn't pinging, we must not advance beyond the pinger's state.
        if ( serviceMeta != null ) {
            logger.trace(methodName, id, "Cumulative before checking monitor/pinger:", response, ".  Monitor state:", serviceMeta.getServiceState());
            if ( serviceMeta.getServiceState().ordinality() <= response.ordinality() ) response = serviceMeta.getServiceState();
        }

        // It can take a while for instance state to catch up with stopping, so we override it here if needed
        if ( isStopped() ) {
            if ( ServiceState.Stopping.ordinality() < response.ordinality() ) {
                logger.info(methodName, id, "Adjust state to", ServiceState.Stopping, "from", response, "because of service stop.");
                response = ServiceState.Stopping;
            }
        }

        return response;
    }

    synchronized ServiceState getState()
    {
        return service_state;
    }

    synchronized void setState(ServiceState new_state, ServiceState cumulative, ServiceInstance si)
    {
        String methodName = "setState";

        String tail = "";
        if ( si == null ) {
            tail = "none/none";            
        } else {
            tail = si.getId() + "/" + si.getState();
        }

        ServiceState prev = this.service_state;
        this.service_state = new_state;
        if ( prev != new_state ) {
            logger.info(methodName, id, "State update from[" + prev + "] to[" + new_state + "] via[" + cumulative + "] Inst[" + tail + "]" );
            saveMetaProperties();
        }

        // Execute actions that must always occur based on the new state
        // These are all idempotent actions, call them as often as you want and no harm.
        switch(new_state) {            
            case Available: 
                startPingThread();
                break;
            case Initializing:
                break;
            case Starting:
                break;
            case Waiting:
                startPingThread();
                break;
            case Stopping:
                stopPingThread();
                break;
            case Stopped:
                stopPingThread();
                break;
            default:
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
                            setState(ServiceState.Waiting, cumulative, si);
                            break;

                        case Stopping:
                            setState(ServiceState.Stopping, cumulative, si);
                            break;

                        case Stopped:
                            setState(ServiceState.Stopped, cumulative, si);
                          break;

                        case Waiting:
                            setState(ServiceState.Initializing, cumulative, si);
                            logger.warn(methodName, id, "ILLEGAL STATE TRANSITION:", getState(), "->", cumulative); 
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
            logger.warn(methodName, id, "Not restarting pinger due to excessiver errors:", ping_failures);
            return;
        }

        try {
            logger.info(methodName, id, "Starting service monitor.");
            serviceMeta = new PingDriver(this);
        } catch ( Throwable t ) {
            logger.error(methodName, id, "Cannot instantiate service monitor.", t);
            return;
        }

        //setState(ServiceState.Waiting);
        Thread t = new Thread(serviceMeta);
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

        if ( ! inShutdown ) {
            saveMetaProperties();         // no i/o during shutdown, it has to be fast and clean
                                          // things will be cleaned up and resynced on restart
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


    synchronized void start(int ninstances)
    {
    	String methodName = "start";

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

                if ( ninstances > 1 ) {
                    ninstances = 1;                                  // and alter the number here.
                    logger.warn(methodName, id, "Adjusting instances to one(1) because process_debug is set.");
                }
            }

            for ( int i = 0; i < ninstances; i++ ) {
                ServiceInstance si = new ServiceInstance(this);
                long instid = -1L;
                logger.info(methodName, id, "Starting instance", i);
                if ( (instid = si.start(props_filename, meta_props)) >= 0 ) {
                    logger.info(methodName, id, "Instance[", i, "] id ", instid);
                    implementors.put(instid, si);
                    handler.addInstance(this, si);
                    signal(si);
                } else {
                    logger.info(methodName, id, "Instance[", i, "] id ", instid, "Failed to start.");
                    setAutostart(false);
                    signal(si);
                    break;
                }
            }        
        }    

        saveMetaProperties();
    }

    /**
     * Stop a specific instance.
     */
    synchronized void stop(Long iid)
    {
        String methodName = "stop(id)";

        logger.info(methodName, id, "Stopping specific instance", iid);

        ServiceInstance si = implementors.get(iid);
        if ( si == null ) {
            logger.warn(methodName, id, "Can't find instance", iid, ", perhaps it's already gone.");
        } else {
            si.stop();
            signal(si);
        }
    }

    /**
     * Stop 'count' services.
     */
    synchronized void stop(int count)
    {
        String methodName = "stop(count)";

        logger.info(methodName, id, "Stopping", count, "implementors");

        if ( count >= implementors.size() ) {
            stopPingThread();
            setStopped();
        }

        for ( ServiceInstance si: implementors.values() ) {
            if ( (count--) > 0 ) {
                si.stop();
                signal(si);
            } else {
                break;
            }
        }
    }

    /**
     * Stop everything
     */
    synchronized void stop()
    {
        stop(implementors.size());
    }

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
            logger.debug(methodName, id, "Lingering stop completes.");
            // doesn't matter how its started i think, we have to set this flag off when we stop
            linger = null;
            stop();
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
        
        ArrayList<ADuccId> imp = new ArrayList<ADuccId>();
        for ( Long id : implementors.keySet() ) {
            // Note: For compatibility with earliver version, we'll send these out as DuccIds, not Longs
            DuccId di = new DuccId(id); 
            imp.add(di);
        }
        sd.setImplementors(imp);

        ArrayList<ADuccId> ref = new ArrayList<ADuccId>();
        ref.clear();
        for ( DuccId id : references.keySet() ) {
            ref.add(id);
        }
        sd.setReferences(ref);

        sd.setInstances(getNInstancesRegistered());

        sd.setType(service_type);
        sd.setSubclass(service_class);
        sd.setEndpoint(endpoint);
        sd.setBroker(broker);
        sd.setServiceState(getState());
        sd.setActive(serviceMeta != null);
        sd.setStopped(stopped);
        sd.setAutostart(autostart);
        sd.setLinger(linger_time);
        sd.setId(id);
        sd.setUser(user);

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

    class Starter
        implements Runnable
    {
        ServiceInstance si;
        Starter(ServiceInstance si)
        {
            this.si = si;
        }

        public void run() {
            si.start(props_filename, meta_props);
        }
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
