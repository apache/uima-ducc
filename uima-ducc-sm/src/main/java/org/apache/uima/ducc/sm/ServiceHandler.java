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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.uima.ducc.cli.DuccServiceApi;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.AServiceRequest;
import org.apache.uima.ducc.transport.event.ServiceDisableEvent;
import org.apache.uima.ducc.transport.event.ServiceEnableEvent;
import org.apache.uima.ducc.transport.event.ServiceIgnoreEvent;
import org.apache.uima.ducc.transport.event.ServiceModifyEvent;
import org.apache.uima.ducc.transport.event.ServiceObserveEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryReplyEvent;
import org.apache.uima.ducc.transport.event.ServiceReplyEvent;
import org.apache.uima.ducc.transport.event.ServiceStartEvent;
import org.apache.uima.ducc.transport.event.ServiceStopEvent;
import org.apache.uima.ducc.transport.event.ServiceUnregisterEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceState;
import org.apache.uima.ducc.transport.event.sm.IServiceDescription;
import org.apache.uima.ducc.transport.event.sm.ServiceDependency;
import org.apache.uima.ducc.transport.event.sm.ServiceMap;




public class ServiceHandler
    implements SmConstants, 
               Runnable
{
    /**
	 * 
	 */
	private DuccLogger logger = DuccLogger.getLogger(ServiceHandler.class.getName(), COMPONENT_NAME);	
    private IServiceManager serviceManager;

    private ServiceStateHandler serviceStateHandler = new ServiceStateHandler();
	private ServiceMap serviceMap = new ServiceMap();       // note this is the sync object for publish

    private Map<DuccId, IDuccWork> newJobs = new HashMap<DuccId, IDuccWork>();
    private Map<DuccId, IDuccWork> newServices = new HashMap<DuccId, IDuccWork>();
    
    private Map<DuccId, IDuccWork> deletedJobs = new HashMap<DuccId, IDuccWork>();
    private Map<DuccId, IDuccWork> deletedServices = new HashMap<DuccId, IDuccWork>();
    
    private Map<DuccId, IDuccWork> modifiedJobs = new HashMap<DuccId, IDuccWork>();
    private Map<DuccId, IDuccWork> modifiedServices = new HashMap<DuccId, IDuccWork>();

    private List<ApiHandler> pendingRequests = new LinkedList<ApiHandler>();
    private Object stateUpdateLock = new Object();

    private Map<String, UiOption> optionMap;    // for modify()

    public ServiceHandler(IServiceManager serviceManager)
    {
        this.serviceManager = serviceManager;        
        Runtime.getRuntime().addShutdownHook(new ServiceShutdown());

        DuccServiceApi dsi = new DuccServiceApi(null);           // instantiate this to access the modify options
        UiOption[] options = dsi.getModifyOptions();

        optionMap = new HashMap<String, UiOption>();
        for ( UiOption o : options ) {
            optionMap.put(o.pname(), o);
        }
    }

    public synchronized void run()
    {
    	String methodName = "run";
        while ( true ) {
            try {
				wait();
			} catch (InterruptedException e) {
				logger.error(methodName, null, e);
			}

            try {
                runCommands();           // enqueued orders that came in while I was away
                processUpdates();
            } catch (Throwable t) {
                logger.error(methodName, null, t);
            }
        }
    }
    /**
     * At boot only ... pass in the set of all known active services to each service so it can update
     * internal state with current published state.
     */
    void bootImplementors(Map<DuccId, DuccWorkJob> incoming)
    {
        for ( DuccId id : incoming.keySet() ) {
            DuccWorkJob j = incoming.get(id);
            String ep = j.getServiceEndpoint();
            ServiceSet sset = serviceStateHandler.getServiceByUrl(ep);
            if ( sset == null ) {
                // must cancel this service, no idea what it is
            } else {
                sset.bootImplementor(id, j.getJobState());                               // boot by id, job, not known so more stuff
                // has to be built up
            }
        }
        List<ServiceSet> services = serviceStateHandler.getServices();
        for ( ServiceSet sset : services ) {
            sset.bootComplete();
        }
    }

    void processUpdates()
    {
    	String methodName = "processUpdates";
        logger.info(methodName, null, "Processing updates.");
        Map<DuccId, IDuccWork> deletedJobsMap      = new HashMap<DuccId, IDuccWork>();
        Map<DuccId, IDuccWork> modifiedJobsMap     = new HashMap<DuccId, IDuccWork>();
        Map<DuccId, IDuccWork> newJobsMap          = new HashMap<DuccId, IDuccWork>();
        Map<DuccId, IDuccWork> deletedServicesMap  = new HashMap<DuccId, IDuccWork>();
        Map<DuccId, IDuccWork> modifiedServicesMap = new HashMap<DuccId, IDuccWork>();
        Map<DuccId, IDuccWork> newServicesMap      = new HashMap<DuccId, IDuccWork>();

        synchronized(stateUpdateLock) {
                deletedJobsMap.putAll(deletedJobs);
                deletedJobs.clear();
                        
                modifiedJobsMap.putAll(modifiedJobs);
                modifiedJobs.clear();
                       
                deletedServicesMap.putAll(deletedServices);
                deletedServices.clear();
                        
                modifiedServicesMap.putAll(modifiedServices);
                modifiedServices.clear();
            
                newServicesMap.putAll(newServices);
                newServices.clear();
                        
                newJobsMap.putAll(newJobs);
                newJobs.clear();
        }

        // We could potentially have several updates where a service or arrives, is modified, and then deleted, while
        // we are busy.  Need to handle them in the right order.  
        //
        // Jobs are dependent on services but not the other way around - I think we need to handle services first,
        // to avoid the case where something is dependent on something that will exist soon but doesn't currently.
        handleNewServices     (newServicesMap     );
        handleModifiedServices(modifiedServicesMap);
        handleDeletedServices (deletedServicesMap );

        handleNewJobs         (newJobsMap         );
        handleModifiedJobs    (modifiedJobsMap    );
        handleDeletedJobs     (deletedJobsMap     );

        List<ServiceSet> regsvcs = serviceStateHandler.getServices();
        for ( ServiceSet sset : regsvcs ) {
            sset.enforceAutostart();
        }

        serviceManager.publish(serviceMap);
    }

    void signalUpdates( // This is the incoming or map, with work split into categories.
                                     // The incoming maps are volatile - must save contents before returning.
                                    HashMap<DuccId, IDuccWork> newJobs, 
                                    HashMap<DuccId, IDuccWork> newServices,                               
                                    HashMap<DuccId, IDuccWork> deletedJobs,
                                    HashMap<DuccId, IDuccWork> deletedServices,                                    
                                    HashMap<DuccId, IDuccWork> modifiedJobs,
                                    HashMap<DuccId, IDuccWork> modifiedServices
                                    )
    {

        synchronized(stateUpdateLock) {
            this.newJobs.putAll(newJobs);            
            this.newServices.putAll(newServices);            
            this.deletedJobs.putAll(deletedJobs);            
            this.deletedServices.putAll(deletedServices);
            this.modifiedJobs.putAll(modifiedJobs);
            this.modifiedServices.putAll(modifiedServices);
        }
        synchronized(this) {
            notify();
        }
    } 

    void runCommands()
    {
        String methodName = "runCommands";
        LinkedList<ApiHandler> tmp = new LinkedList<ApiHandler>();
        synchronized(pendingRequests) {
            tmp.addAll(pendingRequests);
            pendingRequests.clear();
        }
        logger.info(methodName, null, "Running", tmp.size(), "API Tasks.");
        
        synchronized(this) {
            for ( ApiHandler apih : tmp ) {
                apih.run();
            }
        }
    }

    void addApiTask(ApiHandler apih)
    {
        synchronized(pendingRequests) {
            pendingRequests.add(apih);
        }
    }

    /**
     * This is called when an endpoint is referenced as a dependent service from a job or a service.
     * It is called only when a new job or service is first discovred in the OR map.
     */
    protected Map<String, ServiceSet> resolveDependencies(DuccWorkJob w, ServiceDependency s)
    {
    	//String methodName = "resolveDependencies";
    	DuccId id = w.getDuccId();
        String[] deps = w.getServiceDependencies();

        // New services, if any are discovered
        boolean fatal = false;
        Map<String, ServiceSet> jobServices = new HashMap<String, ServiceSet>();
        for ( String dep : deps ) {
            
            // put it into the global map of known services if needed and up the ref count
            ServiceSet sset = serviceStateHandler.getServiceByUrl(dep);
            if ( sset == null ) {

                // Not good.  Lets see if it's a terminating service so we can at least tell the poor guy.
                sset = serviceStateHandler.getUnregisteredServiceByUrl(dep);
                if ( sset == null ) {
                    // Still null, never h'oid of de guy
                    s.addMessage(dep, "Service is unknown.");
                    s.setState(ServiceState.NotAvailable);
                } else {
                    // The service is deregistered but not yet purged, may as well tell him. It can
                    // take a while for these guys to go away.
                    s.addMessage(dep, "Service has been deregistered and is terminating.");
                    s.setState(ServiceState.NotAvailable);
                }
                fatal = true;
                continue;
            }

            jobServices.put(dep, sset);
        }

        if ( fatal ) {
            jobServices.clear();            
        } else {
            for ( ServiceSet sset : jobServices.values() ) {
                // If service is unregistered and then rerigistered while the job is running it may have lost
                // its connections, which we insure we always have here.
                serviceStateHandler.putServiceForJob(w.getDuccId(), sset);
                sset.reference(id);     // might start it if it's not running
            }
        }
        return jobServices;
    }

    /**
     * Resolves state for the job in id based on the what it is dependent upon - the independent services
     *
     * Enter this code ONLY if it is determined that the 'independent' work, 'id', does in fact have
     * declared dependencies.
     *
     * @param id   This is the ID of a job or service we want to work out the service state for
     * @param dep  This is the thing we send to OR telling it about the state of 'id'
     */
    protected void resolveState(DuccId id, ServiceDependency dep)
    {        
        Map<Long, ServiceSet> services = serviceStateHandler.getServicesForJob(id);
        if ( services == null ) {
            dep.setState(ServiceState.NotAvailable);       // says that nothing i need is available
            return;
        }

        ServiceState state = ServiceState.Available;              
        //
        // Start with the most permissive state and reduce it as we walk the list
        // Running > Initializing > Waiting > NotAvailable
        //
        // This sets the state to the min(all dependent service states)
        //
        for ( ServiceSet sset : services.values() ) {
            if ( sset.getState().ordinality() < state.ordinality() ) state = sset.getState();
             dep.setIndividualState(sset.getKey(), sset.getState());
             if ( sset.excessiveFailures() ) {
                 dep.addMessage(sset.getKey(), sset.getErrorString());
             }
             // logger.debug(methodName, id, "Set individual state", sset.getState());
        }
        dep.setState(state);
    }

    /**
     * A job or service has ended.  Here's common code to clean up the dependent services.
     * @param id - the id of the job or service that stopped
     * @param deps - the services that 'id' was dependent upon
     */
    protected void stopDependentServices(DuccId id)
    {
    	String methodName = "stopDependentServices";

        Map<Long, ServiceSet> deps = serviceStateHandler.getServicesForJob(id);
        if ( deps == null ) {
            logger.info(methodName, id, "No dependent services to stop, returning.");
            return;                                              // service already deleted, timing issue
        }

        //
        // Bop through all the things job 'id' is dependent upon, and update their refcounts. If
        // the refs go to 0 we stop the pinger and sometimes the independent service itself. 
        //
        for ( Long depid : deps.keySet() ) {
            logger.debug(methodName, id, "Looking up service", depid);
            
            ServiceSet sset = deps.get(depid);
            if ( sset == null ) {
                logger.error(methodName, id, "Internal error: Null service for " + depid);      // sanity check, should never happen
                continue;
            }
            
            sset.dereference(id);                                    // also maybe stops the pinger

        }

        // last, indicate that job 'id' has nothing it's dependent upon any more
        serviceStateHandler.removeServicesForJob(id);            
    }
 
    protected void handleNewJobs(Map<DuccId, IDuccWork> work)
    { 
        String methodName = "handleNewJobs";

        // Map of updates to send to OR
        HashMap<DuccId, ServiceDependency> updates = new HashMap<DuccId, ServiceDependency>();

        for ( DuccId id : work.keySet() ) {
            DuccWorkJob w = (DuccWorkJob) work.get(id);

            if ( !w.isActive() ) {
                logger.info(methodName, id, "Bypassing inactive job, state =", w.getStateObject());
                continue;
            }

            ServiceDependency s = new ServiceDependency(); // for the OR
            updates.put(id, s);

            String[] deps = w.getServiceDependencies();
            if ( deps == null ) {   // no deps, just mark it running and move on
                s.setState(ServiceState.Available);
                logger.info(methodName, id, "Added to map, no service dependencies.");
                continue;
            }

            Map<String, ServiceSet> jobServices = resolveDependencies(w, s);
            for ( ServiceSet sset : jobServices.values() ) {
                logger.info(methodName, id, "Job is dependent on", sset.getKey());
            }

            resolveState(id, s);
            logger.info(methodName, id, "Added job to map, with service dependency state.", s.getState());

            logger.info(methodName, id, s.getMessages());
        }

        serviceMap.putAll(updates);

    }

    protected void handleModifiedJobs(Map<DuccId, IDuccWork> work)
    {
        String methodName = "handleModifiedobs";

        //
        // Only look at active jobs.  The others will be going away soon and we use
        // that time as a grace period to keep the management machinery running in 
        // case more work comes in in the next few minutes.
        //
        // Everything is already in the service map so we just update the state.
        //
        for ( DuccId id : work.keySet() ) {
            
            DuccWorkJob j = (DuccWorkJob) work.get(id);
            String[] deps = j.getServiceDependencies();
            if ( deps == null ) {   // no deps, just mark it running and move on
                logger.info(methodName, id, "No service dependencies, no updates made.");
                continue;
            }

            ServiceDependency s = serviceMap.get(id);
            if ( j.isFinished() ) {
                stopDependentServices(id);
                s.setState(ServiceState.NotAvailable);
                s.clearMessages();
            } else  if ( j.isActive() ) {
                resolveDependencies(j, s);
                resolveState(id, s);
            } 
        }

    }

    protected void handleDeletedJobs(Map<DuccId, IDuccWork> work)
    {
        String methodName = "handleDeletedobs";

        for ( DuccId id : work.keySet() ) {
            DuccWorkJob w = (DuccWorkJob) work.get(id);
            
            String[] deps = w.getServiceDependencies();
            if ( deps == null ) {   // no deps, just mark it running and move on
                logger.info(methodName, id, "No service dependencies, no updates made.");
                continue;
            }

            stopDependentServices(id);

            logger.info(methodName, id, "Deleted job from map");
        }

        serviceMap.removeAll(work.keySet());

    }

    protected void handleNewServices(Map<DuccId, IDuccWork> work)
    {
        String methodName = "handleNewServices";

        Map<DuccId, ServiceDependency> updates     = new HashMap<DuccId, ServiceDependency>();   // to be added to the service map sent to OR

        for ( DuccId id : work.keySet() ) {
            DuccWorkJob w = (DuccWorkJob) work.get(id);

            //
            // On restart we sometimes get stale stuff that we just ignore.
            //  
            if ( !w.isActive() ) {
                logger.info(methodName, id, "Bypassing inactive service, state=", w.getStateObject());
                continue;
            }

            ServiceDependency s = new ServiceDependency();
            updates.put(id, s);

            String endpoint = w.getServiceEndpoint();
            if ( endpoint == null ) {                                     // the job is damaged if this happens
                String msg = "No service endpoint.  Service cannot be validated.";
                logger.warn(methodName, id, msg);
                s.addMessage("null", msg);                                // this is a fatal state always
                s.setState(ServiceState.NotAvailable);
                continue;
            }

            String[] deps = w.getServiceDependencies();                  // other services this svc depends on
            ServiceSet sset = serviceStateHandler.getServiceByImplementor(id.getFriendly());
            if ( sset == null ) {
                s.addMessage(endpoint, "No registered service for " + endpoint);
                s.setState(ServiceState.NotAvailable);
                continue;
            } 

            //
            // No deps.  Put it in the map and move on.
            //
            if ( deps == null ) {                
                logger.info(methodName, id, "Added service to map, no service dependencies. ");
                s.setState(ServiceState.Available);                        // good to go in the OR (the state of things i'm dependent upon)
                sset.signalUpdate(w);
                continue;
            }

            resolveDependencies(w, s);                                     // check what I depend on and maybe kick 'em
            resolveState(id, s);                                           // get cumulative state based on my deps
            
            sset.signalUpdate(w);                       // kick my own instance
            logger.info(methodName, id, "Added to map, with service dependencies,", s.getState());
        }

        serviceMap.putAll(updates);                                        // for return to OR
    }

    /**
     * The assumption here is that we already had the service instance in our map, and OR is
     * delivering an update.  That means the instance was known to us in the past, it is not new.
     */
    protected void handleModifiedServices(Map<DuccId, IDuccWork> work)
    {
        String methodName = "handleModifiedServices";
        
        //
        // This is a specific service process, but not necessarily the whole service.
        //
        for ( DuccId id : work.keySet() ) {
            DuccWorkJob w = (DuccWorkJob) work.get(id);
            String url = w.getServiceEndpoint();
            
            IDuccProcessMap pm = w.getProcessMap();
            String node = "<unknown>";
            Long share_id = -1L;
            if ( pm.size() > 1 ) {
                logger.warn(methodName, id, "Process map is too large, should be size 1.  Size:", pm.size(), "Cannot determine node or share_id for service.");
            } else if ( pm.size() < 1 ) {
                logger.warn(methodName, id, "Process map is empty but we are expecting exactly one entry. Cannot determine node or share id for service.");
            } else {
                for ( DuccId pid : pm.keySet() ) {
                    NodeIdentity ni = pm.get(pid).getNodeIdentity();
                    node = ni.getName();
                    share_id = pid.getFriendly();
                }               
            }         
            
            if (url == null ) {              // probably impossible but lets not chance NPE
                logger.warn(methodName, id, "Missing service endpoint/url, ignoring.");
                continue;
            }

            ServiceSet sset = serviceStateHandler.getServiceByImplementor(id.getFriendly());
            if ( sset == null ) {
                sset = serviceStateHandler.getUnregisteredServiceByUrl(url);
                if ( sset == null ) {
                    // leftover junk publication maybe? can't tell
                    logger.info(methodName, id, "Update for active service instance", id.toString(), 
                                "but have no registration for it. Job state:", w.getJobState());
                    continue;
                }
                logger.info(methodName, id, "Update for unregistered service, continuing shutdown of service. Job State:", w.getJobState());
            }
            
            if ( !sset.containsImplementor(id) ) {
                if ( !sset.canDeleteInstance(w) ) {
                    // the instance isn't dead, this is a possible problem
                    logger.warn(methodName, id, "sset for", sset.getId(), "does not contain instance");
                }
                continue;      // we don't care any more, he's gone
            }
        
            if ( share_id != -1 ) {
                sset.updateInstance(id.getFriendly(), share_id, node);
            }
            ServiceDependency s = serviceMap.get(id);
            if ( w.isFinished() ) {              // nothing more, just dereference and maybe stop stuff I'm dependent upon
                // state Completing or Completed
                stopDependentServices(id);
                s.setState(ServiceState.NotAvailable);              // tell orchestrator
            } else if ( w.getServiceDependencies() != null ) {      // update state from things I'm dependent upon
                resolveDependencies(w, s);
                resolveState(id, s);
            }

            sset.signalUpdate(w);
        }

    }

    protected void handleDeletedServices(Map<DuccId, IDuccWork> work)

    {
        String methodName = "handleDeletedServices";

        for ( DuccId id : work.keySet() ) {
        	DuccWorkJob w = (DuccWorkJob) work.get(id);
        	String url = w.getServiceEndpoint();
            logger.info(methodName, id, "Instance deleted for", url);

            if (url == null ) {              // probably impossible but lets not chance NPE
                logger.warn(methodName, id, "Missing service endpoint, ignoring.");
                continue;
            }
            
            // 
            // Dereference and maybe stop the services I'm dependent upon
            //
            if ( w.getServiceDependencies() == null ) { 
                logger.info(methodName, id, "No service dependencies to update on removal.");
            } else {
                stopDependentServices(id);        // update references, remove implicit services if any
            }

            ServiceSet sset = serviceStateHandler.getServiceByImplementor(id.getFriendly());
            if ( sset != null ) {       // can happen on unregister
                sset.signalUpdate(w);
            }
        }

        serviceMap.removeAll(work.keySet());                          // and finally the deleted services
    }

    /**
     * Add in the service dependencies to the query.
     */
    void updateServiceQuery(IServiceDescription sd, ServiceSet sset)
    {
        // 
        // The thing may not be running yet / at-all.  Pull out the deps from the registration and
        // query them individually.
        //
        String[] deps = sset.getIndependentServices();
        if ( deps != null ) {
            for ( String dep : deps ) {
                ServiceSet independent = serviceStateHandler.getServiceByUrl(dep);
                if ( independent != null ) {
                    sd.addDependency(dep, independent.getState().decode());
                } else {
                    sd.addDependency(dep, ServiceState.Stopped.decode());
                }
            }
        }
    }

    ServiceQueryReplyEvent query(ServiceQueryEvent ev)
    {
    	//String methodName = "query";
        long   id     = ev.getFriendly();
        String url    = ev.getEndpoint();
        ServiceQueryReplyEvent reply = new ServiceQueryReplyEvent();

        if (( id == -1) && ( url == null )) {
            for ( ServiceSet sset : serviceStateHandler.getServices()) {
                IServiceDescription sd = sset.query();
                updateServiceQuery(sd, sset);
                reply.addService(sd);
            }
        } else {
            ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
            if ( sset == null ) {
                reply.setMessage("Unknown service");
                reply.setEndpoint(url);
                reply.setReturnCode(false);
            } else {
                IServiceDescription sd = sset.query();
                updateServiceQuery(sd, sset);
                reply.addService(sd);
            }
        } 

        return reply;
    }

    boolean authorized(String operation, ServiceSet sset, AServiceRequest req)
    {
        String methodName = "authorized";

        String userin  = req.getUser();
        String userout = sset.getUser();

        if ( userin.equals(userout) ) {                  // owner is always authorized
            logger.info(methodName, sset.getId(), operation, "request from", userin, "allowed.");
            return true;
        }

        if ( serviceManager.isAdministrator(req) ) {      // global admin is always authorized
            logger.info(methodName, sset.getId(), operation, "request from", userin, "allowed as DUCC administrator. Service owner:", userout);
            return true;
        }

        if ( sset.isAuthorized(userin) ) {                  // registered co-owner is always authorized
            logger.info(methodName, sset.getId(), operation, "request from", userin, "alloed as co-ownder.  Service owner:", userout);
            return true;
        }

        logger.info(methodName, sset.getId(), operation, "request from", userin, "not authorized.  Service owner:", userout);
        return false;
    }

    ServiceReplyEvent start(ServiceStartEvent ev)
    {
        // String methodName = "start";
        
        long   id  = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return new ServiceReplyEvent(false, "Unknown service", url, id);
        }

        if ( ! authorized("start", sset, ev) ) {
            return new ServiceReplyEvent(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }


        int running    = sset.countImplementors();
        int instances  = ev.getInstances();

        if ( (instances == -1) && !sset.enabled() ) {    // no args always enables
            sset.enable();
        } else if ( ! sset.enabled() ) {
            return new ServiceReplyEvent(false, "Service is disabled, cannot start (" + sset.getDisableReason() + ")", url, sset.getId().getFriendly());
        }

        if ( sset.isDebug() ) {
            if ( sset.countImplementors() > 0 ) {
                return new ServiceReplyEvent(true, 
                                             "Already has instances[" + running + "] and service has process_debug set - no additional instances started", 
                                             sset.getKey(), 
                                             sset.getId().getFriendly());
            }
        }

        int registered = sset.getNInstancesRegistered();
        int wanted     = 0;
        
        if ( instances == -1 ) {
            wanted = Math.max(0, registered - running);
        } else {
            wanted = instances;
        }
        
        if ( wanted == 0 ) {
            return new ServiceReplyEvent(true, 
                                         "Already has instances[" + running + "] - no additional instances started", 
                                         sset.getKey(), 
                                         sset.getId().getFriendly());
        }
        
        pendingRequests.add(new ApiHandler(ev, this));

        if ( sset.isDebug() && (wanted > 1) ) {
            return new ServiceReplyEvent(true, 
                                         "Instances adjusted to [1] because process_debug is set",
                                         sset.getKey(), 
                                         sset.getId().getFriendly());
        } else {
            return new ServiceReplyEvent(true, 
                                         "New instances[" + wanted + "]", 
                                         sset.getKey(), 
                                         sset.getId().getFriendly());
        }
    }

    //
    // Everything to do this must be vetted before it is called
    //
    // Start with no instance says: start enough new processes to get up the registered amount
    // Start with some instances says: start exactly this many
    // If the --save option is included, also update the registration
    //
    void doStart(ServiceStartEvent ev)
    {
    	String methodName = "doStart";

        long friendly = ev.getFriendly();
        String epname = ev.getEndpoint();
        int instances = ev.getInstances();
        ServiceSet sset = serviceStateHandler.getServiceForApi(friendly, epname);

        int running    = sset.countImplementors();
        int registered = sset.getNInstancesRegistered();
        int wanted     = 0;

        if ( sset.isDebug() ) {
            if ( sset.countImplementors() > 0  ) {
                logger.warn(methodName, sset.getId(), "Not starting additional instances because process_debug is set.");
                return;
            }
            
            if ( instances > 1 ) {
                logger.warn(methodName, sset.getId(), "Adjusting instances to [1] because process_debug is set.");
                instances = 1;
            }
        }

        if ( instances == -1 ) {
            wanted = Math.max(0, registered - running);
        } else {
            wanted = instances;
        }

        sset.resetRuntimeErrors();
        sset.setStarted();                              // manual start overrides, if there's still a problem
        sset.updateInstances(running + wanted); // pass in target instances
    }

    ServiceReplyEvent stop(ServiceStopEvent ev)
    {
        String methodName = "stop";

        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return new ServiceReplyEvent(false, "Unknown service", url, id);
        }

        if ( ! authorized("stop", sset, ev) ) {
            return new ServiceReplyEvent(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( sset.isStopped() ) {
            return new ServiceReplyEvent(false, "Already stopped", sset.getKey(), sset.getId().getFriendly());
        }

        int running    = sset.countImplementors();
        int instances  = ev.getInstances();
        int tolose;
        String msg;
        // CLI/API prevents instances < -1
        if ( instances == -1 ) {                             // figure out n to lose
            tolose = running;
            msg = "Stopping all deployments.";
        } else {
            tolose = Math.min(instances, running);
            msg = "Stopping " + tolose + " deployments.";
        }

        logger.info(methodName, sset.getId(), msg);
        pendingRequests.add(new ApiHandler(ev, this));
        return new ServiceReplyEvent(true, msg, sset.getKey(), sset.getId().getFriendly());
    }

    //
    // Everything to do this must be vetted before it is called
    //
    // If instances == 0 set stop the whole service
    // Otherwise we just stop the number asked for
    // If --save is insicated we update the registry
    //
    void doStop(ServiceStopEvent event)
    //long id, String url, int instances)
    {
        //String methodName = "doStop";

        int instances = event.getInstances();
        long id = event.getFriendly();
        String url = event.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);

        int running    = sset.countImplementors();
        int tolose;
        
        // CLI/API prevents instances < -1
        if ( instances == -1 ) {                             // figure out n to lose
            if ( sset.isAutostart() || sset.isReferencedStart() ) {
                sset.disableAndStop("Disabled by stop from id " + event.getUser());
            } else {
                sset.stop(running);
            }
        } else {
            tolose = Math.min(instances, running);
            sset.updateInstances(Math.max(0, running - tolose)); // pass in target intances running
        }

    }

    ServiceReplyEvent disable(ServiceDisableEvent ev)
    {
        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return new ServiceReplyEvent(false, "Unknown service", url, id);
        }

        if ( ! authorized("disable", sset, ev) ) {
            return new ServiceReplyEvent(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( !sset.enabled() ) {
            return new ServiceReplyEvent(true, "Service is already disabled", sset.getKey(), sset.getId().getFriendly());
        }

        sset.disable("Disabled by owner or administrator " + ev.getUser());
        sset.saveMetaProperties();
        return new ServiceReplyEvent(true, "Disabled", sset.getKey(), sset.getId().getFriendly());
    }

    ServiceReplyEvent enable(ServiceEnableEvent ev)
    {
        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return new ServiceReplyEvent(false, "Unknown service", url, id);
        }

        if ( ! authorized("enable", sset, ev) ) {
            return new ServiceReplyEvent(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( sset.enabled() ) {
            return new ServiceReplyEvent(true, "Service is already enabled", sset.getKey(), sset.getId().getFriendly());
        }

        sset.enable();
        sset.saveMetaProperties();
        return new ServiceReplyEvent(true, "Enabled.", sset.getKey(), sset.getId().getFriendly());
    }


    ServiceReplyEvent ignore(ServiceIgnoreEvent ev)
    {
        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return new ServiceReplyEvent(false, "Unknown service", url, id);
        }

        if ( ! authorized("ignore", sset, ev) ) {
            return new ServiceReplyEvent(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( sset.isAutostart() ) {
            return new ServiceReplyEvent(false, "Service is autostarted, ignore-references not applied.", sset.getKey(), sset.getId().getFriendly());
        }

        if ( !sset.isReferencedStart() ) {
            return new ServiceReplyEvent(true, "Service is already ignoring references", sset.getKey(), sset.getId().getFriendly());
        }

        
        sset.ignoreReferences();
        return new ServiceReplyEvent(true, "References now being ignored.", sset.getKey(), sset.getId().getFriendly());
    }

    ServiceReplyEvent observe(ServiceObserveEvent ev)
    {
        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return new ServiceReplyEvent(false, "Unknown service", url, id);
        }

        if ( ! authorized("observe", sset, ev) ) {
            return new ServiceReplyEvent(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( sset.isAutostart() ) {
            return new ServiceReplyEvent(false, "Must set autostart off before enabling reference-starts.", sset.getKey(), sset.getId().getFriendly());
        }
        
        sset.enable();
        sset.observeReferences();
        return new ServiceReplyEvent(true, "Observing references.", sset.getKey(), sset.getId().getFriendly());
    }

    ServiceReplyEvent register(DuccId id, String props_filename, String meta_filename, DuccProperties props, DuccProperties meta)
    {
    	String methodName = "register";

        String error = null;
        boolean must_deregister = false;

        String url = meta.getProperty("endpoint");
        ServiceSet sset = serviceStateHandler.getServiceByUrl(url);
        if (sset != null ) {
            error = "Duplicate registered by " + sset.getUser();
            return new ServiceReplyEvent(false, error, url, sset.getId().getFriendly());
        }

        try {
            sset = new ServiceSet(this, id, props_filename, meta_filename, props, meta);
        } catch (Throwable t) {
            // throws because endpoint is not parsable
            error = t.getMessage();
            return new ServiceReplyEvent(false, error, url, id.getFriendly());            
        }

        try {
            sset.saveServiceProperties();
        } catch ( Exception e ) {
            error = ("Internal error; unable to store service descriptor. " + url); 
            logger.error(methodName, id, e);
            must_deregister = true;
        }
        
        try {
            if ( ! must_deregister ) {
                sset.saveMetaProperties();
            }
        } catch ( Exception e ) {
            error = ("Internal error; unable to store service meta-descriptor. " + url);
            logger.error(methodName, id, e);
            must_deregister = true;
        }

        // must check for cycles or we can deadlock
        if ( ! must_deregister ) {
            // TODO R2, revive the cycle checker
            //                 CycleChecker cc = new CycleChecker(sset);
            //                 if ( cc.hasCycle() ) {
            //                     error = ("Service dependencies contain a cycle with " + cc.getCycles());
            //                     logger.error(methodName, id, error);
            //                     must_deregister = true;
            //                 }
        }

        if ( error == null ) {
            serviceStateHandler.registerService(id.getFriendly(), url, sset);
            return new ServiceReplyEvent(true, "Registered", url, id.getFriendly());
        } else {
            File mf = new File(meta_filename);
            mf.delete();
            
            File pf = new File(props_filename);
            pf.delete();
            return new ServiceReplyEvent(false, error, url, id.getFriendly());
        }
    }

    public ServiceReplyEvent modify(ServiceModifyEvent ev)
    {
        long  id   = ev.getFriendly();
        String url = ev.getEndpoint();
    	ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return new ServiceReplyEvent(false, "Unknown service", url, id);
        }

        if ( ! authorized("modify", sset, ev) ) {
            return new ServiceReplyEvent(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }
        
        pendingRequests.add(new ApiHandler(ev, this));
        return new ServiceReplyEvent(true, "Modifying", sset.getKey(), sset.getId().getFriendly());
    }

    boolean restart_pinger = false;
    boolean restart_service = false;

    void modifyRegistration(ServiceSet sset, UiOption option, String value)
    {

        int     intval = 0;
        boolean boolval = false;

        // TODO: this case covers ALL service options, but note that only those in the modify list
        //       in the CLI are actually used.  Eventually we will cover them all.
        switch ( option ) {
            case Instances:
                intval = Integer.parseInt(value);                
                sset.updateRegisteredInstances(intval);
                break;

            case Autostart:
                boolval = Boolean.parseBoolean(value);
                sset.setAutostart(boolval);
                break;

            case Administrators:
                sset.setJobProperty(option.pname(), value);
                sset.parseAdministrators(value);
                break;

            // For the moment, these all update the registration but don't change internal 
            // operation.
            case Description:
            case LogDirectory:
            case Jvm:
            case ProcessJvmArgs:
            case Classpath:
            case SchedulingClass:
            case Environment:
            case ProcessMemorySize:           
            case ProcessExecutable:
            case ProcessExecutableArgs:
            case ClasspathOrder:
            case ServiceDependency:
            case ProcessInitializationTimeMax:
            case WorkingDirectory:
                sset.setJobProperty(option.pname(), value);
                break;

            case InstanceInitFailureLimit:
                sset.updateInitFailureLimit(value);
                sset.setJobProperty(option.pname(), value);
                break;

            case ServiceLinger:
                sset.updateLinger(value);
                sset.setJobProperty(option.pname(), value);
                break;

            case ProcessDebug:
                // Note this guy updates the props differently based on the value
                sset.updateDebug(value);      // value may be numeric, or "off" 
                break;

            case ServicePingArguments:
            case ServicePingClasspath:
            case ServicePingJvmArgs:
            case ServicePingTimeout:
            case ServicePingDoLog:
            case ServicePingClass:
            case InstanceFailureWindow:
            case InstanceFailureLimit:
                if ( value.equals("default") ) {
                    sset.deleteJobProperty(option.pname());
                } else {
                    sset.setJobProperty(option.pname(), value);
                }
                restart_pinger = true;
                break;

        }
    }

    //void doModify(long id, String url, int instances, Trinary autostart, boolean activate)
    void doModify(ServiceModifyEvent sme)
    {
        String methodName = "doModify";

        long id = sme.getFriendly();
        String url = sme.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);

        DuccProperties mods  = sme.getProperties();
        restart_pinger = false;
        restart_service = false;
        Set<String> keys = mods.stringPropertyNames();        

        for (String kk : keys ) {
            UiOption k = optionMap.get(kk);

            if ( k == null ) {
            	logger.debug(methodName, sset.getId(), "Bypass property", kk);
            	continue;
            }
            
            switch ( k ) {
                case Help:
                case Debug:
                case Modify:
                    // used by CLI only, won't even be passed in
                    continue;
                case Activate:
                    // TODO: I don't think this is ever used.  Maybe just drop it?
                    continue;
            }

            String v = (String) mods.get(kk);
            try {
            	modifyRegistration(sset, k, v);
            } catch ( Throwable t ) {
                logger.error(methodName, sset.getId(), "Modify", kk, "to", v, "Failed:", t);
                continue;
            }

            logger.info(methodName, sset.getId(), "Modify", kk, "to", v, "restart_service[" + restart_service + "]", "restart_pinger[" + restart_pinger + "]");
        }
        
        sset.resetRuntimeErrors();
        sset.saveServiceProperties();
        sset.saveMetaProperties();

        if ( restart_pinger ) {
            sset.restartPinger();
            restart_pinger = false;
        }

        // restart_service - not yet
    }

    public ServiceReplyEvent unregister(ServiceUnregisterEvent ev)
    {
        //String methodName = "unregister";
        long id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return new ServiceReplyEvent(false, "Unknown service",  url, id);
        }

        id = sset.getId().getFriendly();           // must insure the ev has the numeric id because we work entirely with that from now ow
        url = sset.getKey();         // also insure url is there for messages
        ev.setEndpoint(url);
        ev.setFriendly(id);

        if ( ! authorized("unregister", sset, ev) ) {
            return new ServiceReplyEvent(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }
        
        serviceStateHandler.unregister(sset);
        sset.deregister();          // just sets a flag so we know how to handle it when it starts to die
        pendingRequests.add(new ApiHandler(ev, this));
        return new ServiceReplyEvent(true, "Shutting down implementors", sset.getKey(), sset.getId().getFriendly());
    }

    //
    // Everything to do this must be vetted before it is called. Run in a new thread to not hold up the API.
    //
    void doUnregister(ServiceUnregisterEvent ev)
    {
    	String methodName = "doUnregister";
        long friendly = ev.getFriendly();
        String url = ev.getEndpoint();

        ServiceSet sset = serviceStateHandler.getUnregisteredService(friendly);
        if ( sset == null ) {
            logger.error(methodName, null, "Service", friendly, "(" + url + ") is not a known, unregistereed service. No action taken.");
            return;
        }

        sset.disableAndStop("Disabled by unregister from id " + ev.getUser());
        if ( sset.isPingOnly() ) {
            logger.info(methodName, sset.getId(), "Unregister ping-only setvice:", friendly, url);
            serviceStateHandler.removeService(sset);
            sset.deleteProperties();
        } else if ( sset.countImplementors() > 0 ) {
            logger.debug(methodName, sset.getId(), "Stopping implementors:", friendly, url);
        } else {
            logger.debug(methodName, sset.getId(), "Removing from map:", friendly, url);
            sset.clearQueue();       // will call removeServices if everything looks ok
        }

    }

    void addInstance(ServiceSet sset, ServiceInstance inst)
    {
        serviceStateHandler.addImplementorFor(sset, inst);
    }

    void removeImplementor(ServiceSet sset, ServiceInstance inst)
    {
        serviceStateHandler.removeImplementorFor(sset, inst);
    }

    void removeService(ServiceSet sset)
    {
       String methodName = "deleteService";
       if ( serviceStateHandler.hasService(sset.getId()) ) {
           logger.error(methodName, sset.getId(), "Attempt to delete service while it is still registered: refused.");
       } else {
           serviceStateHandler.removeService(sset);
       }
    }

    /**
     * From: http://en.wikipedia.org/wiki/Topological_sorting
     *
     * L Empty list that will contain the sorted elements
     * S Set of all nodes with no incoming edges
     * while S is non-empty do
     *     remove a node n from S
     *     insert n into L
     *     for each node m with an edge e from n to m do
     *         remove edge e from the graph
     *         if m has no other incoming edges then
     *             insert m into S
     * if graph has edges then
     *     return error (graph has at least one cycle)
     * else 
     *     return L (a topologically sorted order)
     */
//     class CycleChecker
//     {
//         ServiceSet sset;
//         int edges = 0;
//         List<String> cycles = null;

//         CycleChecker(ServiceSet sset)
//         {
//             this.sset = sset;
//         }

//         boolean hasCycle()
//         {
//             // Start by building the dependency graph
//             // TODO: Maybe consider saving this.  Not clear there's much of a
//             //       gain doing the extra bookeeping beause the graphs will always
//             //       be small and will only need checking on registration or arrival
//             //       of a submitted service.  So this cycle checking is always
//             //       fast anyway.
//             //
//             //       Bookeeping could be a bit ugly because a submitted service could
//             //       bop in and change some dependency graph.  We really only care
//             //       for checking cycles, so we'll check the cycles as things change
//             //       and then forget about it.
//             //
//             String[] deps = sset.getIndependentServices();
//             if ( deps == null ) return false;          // man, that was fast!

//             Map<String, ServiceSet> visited = new HashMap<String, ServiceSet>();     // all the nodes in the graph
//             clearEdges(sset, visited);

//             List<ServiceSet> nodes = new ArrayList<ServiceSet>();
//             nodes.addAll(visited.values());
//             buildGraph(nodes);
            
//             List<ServiceSet>        sorted = new ArrayList<ServiceSet>();          // topo-sorted list of nodes
//             List<ServiceSet>        current = new ArrayList<ServiceSet>();         // nodes with no incoming edges

//             // Constant: current has all nodes with no incoming edges
//             for ( ServiceSet node : nodes ) {
//                 if ( ! node.hasPredecessor() ) current.add(node);
//             }

//             while ( current.size() > 0 ) {
//                 ServiceSet next = current.remove(0);                            // remove a node n from S
//                 sorted.add(next);                                               // insert n int L
//                 List<ServiceSet> successors = next.getSuccessors();
//                 for ( ServiceSet succ : successors ) {                          // for each node m(pred) with an edge e from n to m do
//                     next.removeSuccessor(succ);                                 // remove edge from graph
//                     succ.removePredecessor(next);                               //    ...
//                     edges--;
//                     if ( !succ.hasPredecessor() ) current.add(succ);            // if m(pred) has no incoming edges insert m into S
//                 }
//             }

//             if ( edges == 0 ) return false;                                     // if graph has no edges, no cycles

//             cycles = new ArrayList<String>();                                   // oops, and here they are
//             for ( ServiceSet node : nodes ) {
//                 if ( node.hasSuccessor() ) {
//                     for ( ServiceSet succ : node.getSuccessors() ) {
//                         cycles.add(node.getKey() + " -> " + succ.getKey());
//                     }
//                 }
//             }
//             return true;
//         }

//         String getCycles()
//         {
//             return cycles.toString();
//         }
        
//         //
//         // Traveerse the graph and make sure all the nodes are "clean" 
//         //
//         void clearEdges(ServiceSet node, Map<String, ServiceSet> visited)
//         {
//             String key = node.getKey();
//             node.clearEdges();
//             if ( visited.containsKey(key) ) return;

//             visited.put(node.getKey(), node);
//             String[] deps = node.getIndependentServices();
//             if ( deps == null ) return;
            
//             for ( String dep : deps ) {
//                 ServiceSet sset = serviceStateHandler.getServiceByName(dep);
//                 if ( sset != null ) {
//                 	clearEdges(sset, visited);
//                 }
//             }
//         }
            
//         void buildGraph(List<ServiceSet> nodes)
//         {            
//             for ( ServiceSet node : nodes ) {           
//                 String[] deps = node.getIndependentServices();           // never null if we get this far
//                 if ( deps != null ) {
//                     for ( String d : deps ) {
//                         ServiceSet outgoing = serviceStateHandler.getServiceByName(d);
//                         if ( outgoing == null ) continue;
//                         outgoing.setIncoming(node);
//                         node.setOutgoing(outgoing);
//                         edges++;
//                     }
//                 }
//             }
//         }
//     }

    /**
     * This is the shutdown hook that stops all the pingers.
     */
    class ServiceShutdown
        extends Thread
    {
        ServiceShutdown() 
        {
        }

        public void run()
        {
            List<ServiceSet> allServices = serviceStateHandler.getServices();
            for (ServiceSet sset : allServices) {
                sset.stopMonitor();
            }            
        }

    }

     class ServiceStateHandler
     {

//         // Map of active service descriptors by endpoint.  For UIMA services, key is the endpoint.
         private Map<String,  ServiceSet>  registeredServicesByUrl         = new HashMap<String,  ServiceSet>();
         private Map<Long,    ServiceSet>  registeredServicesById          = new HashMap<Long,    ServiceSet>();
         private Map<Long,    ServiceSet>  unregisteredServicesById        = new HashMap<Long,    ServiceSet>();
         private Map<String,  ServiceSet>  unregisteredServicesByUrl       = new HashMap<String,  ServiceSet>();

         private Map<Long,    ServiceSet>  servicesByImplementor           = new HashMap<Long, ServiceSet>();

         //         private Map<Long,    ServiceSet>  servicesByFriendly = new HashMap<Long,    ServiceSet>();

//         // For each job, the collection of services it is dependent upon
//         // DUccId is a Job Id (or id for serice that has dependencies)
         private Map<DuccId, Map<Long, ServiceSet>>  servicesByJob = new HashMap<DuccId, Map<Long, ServiceSet>>();

//         ServiceStateHandler()
//         {
//         }

//         /**
//          * Return a copy of the keys so we can fetch the services in an orderly manner.
//          */
//         synchronized ArrayList<String> getServiceNames()
//         {
//             ArrayList<String> answer = new ArrayList<String>();
//             for ( String k : servicesByName.keySet() ) {
//                 answer.add(k);
//             }
//             return answer;
//         }

         synchronized void unregister(ServiceSet sset)
         {
        	 String methodName = "ServiceStateHandler.unregister";
             String key = sset.getKey();
             long   fid = sset.getId().getFriendly();
             logger.info(methodName, sset.getId(), "Removing", key, fid);
             registeredServicesByUrl.remove(key);
             registeredServicesById.remove(fid);

             unregisteredServicesById.put(fid, sset);
             unregisteredServicesByUrl.put(key, sset);
         }

//          synchronized ServiceSet getUnregisteredService(String url)
//          {
//              return unRegisteredServicesByUrl.get(url);
//          }

         synchronized boolean hasService(DuccId id)
         {
        	 String methodName = "ServiceStateHandler.hasService";

             logger.info(methodName, null, "containsKey", id, registeredServicesById.containsKey(id.getFriendly()));
             return registeredServicesById.containsKey(id.getFriendly());
         }

         synchronized void registerService(Long id, String ep, ServiceSet sset)
         {
        	 String methodName = "ServiceStateHandler.registerService";

             logger.info(methodName, sset.getId(), "adding", ep, id);

             registeredServicesByUrl.put(ep, sset);
             registeredServicesById.put(id, sset);
         }

         synchronized ServiceSet getServiceByUrl(String n)
         {
             return registeredServicesByUrl.get(n);
         }

         synchronized List<ServiceSet> getServices()
         {
             ArrayList<ServiceSet> answer = new ArrayList<ServiceSet>();
             for ( ServiceSet sset : registeredServicesByUrl.values() ) {
                 answer.add(sset);
             }
             return answer;
         }

         synchronized void addImplementorFor(ServiceSet sset, ServiceInstance inst)
         {
             servicesByImplementor.put(inst.getId(), sset);
         }

         synchronized ServiceSet getServiceByImplementor(long id)
         {
             return servicesByImplementor.get(id);
         }

         synchronized void removeImplementorFor(ServiceSet sset, ServiceInstance inst)
         {
             servicesByImplementor.remove(inst.getId());
         }

//         synchronized ServiceSet getServiceByFriendly(long id)
//         {
//             return servicesByFriendly.get( id );
//         }

         // API passes in a friendly (maybe) and an endpiont (maybe) but only one of these
         // Here we look up the service by whatever was passed in.
         synchronized ServiceSet  getServiceForApi(long id, String n)
         {
             if ( n == null ) return registeredServicesById.get(id);
             return registeredServicesByUrl.get(n);
         }

         synchronized ServiceSet getUnregisteredService(long id)
         {
             return unregisteredServicesById.get(id);
         }
         
         synchronized ServiceSet getUnregisteredServiceByUrl(String url)
         {
             return unregisteredServicesByUrl.get(url);
         }
         

//         synchronized void putServiceByName(String n, ServiceSet s)
//         {
//             servicesByName.put(n, s);
//             DuccId id = s.getId();
//             if ( id != null ) {
//                 servicesByFriendly.put(id.getFriendly(), s);
//             }
//         }


         synchronized void removeService(ServiceSet sset)
         {
             String key = sset.getKey();
             long id = sset.getId().getFriendly();
             unregisteredServicesById.remove(id);
             unregisteredServicesByUrl.remove(key);

             // The registeredServices need to have been removed during unregister which is the only way
             // to get rid of a service.
             Long[] implids = sset.getImplementors();
             for ( long l : implids ) {
                 servicesByImplementor.remove(l);
             }

             DuccId[] refids = sset.getReferences();
             for ( DuccId rid : refids) {
                 servicesByJob.remove(rid);
             }
         }

//         synchronized void removeService(long id)
//         {
//             ServiceSet sset = servicesByFriendly.remove(id);
//             if ( sset != null ) {
//                 String key = sset.getKey();
//                 servicesByName.remove(key);
//             }
//         }

//         synchronized void removeService(String n, long id)
//         {
//             if ( n == null ) removeService(id);
//             else             removeService(n);
//         }

         synchronized Map<Long, ServiceSet> getServicesForJob(DuccId id)
         {             
             return servicesByJob.get(id);
         }
         
         synchronized void putServiceForJob(DuccId id, ServiceSet s)
         {
             Map<Long, ServiceSet> services = servicesByJob.get(id);
             if ( services == null ) {
                 services = new HashMap<Long, ServiceSet>();
                 servicesByJob.put(id, services);
             }
             services.put(s.getId().getFriendly(), s);
         }

         synchronized void removeServicesForJob(DuccId id)
         {
             servicesByJob.remove(id);
         }

//         synchronized void recordNewServices(Map<String, ServiceSet> services) 
//         {
//             servicesByName.putAll(services);
//         }

     }


    // tester for the topo sorter
    public static void main(String[] args)
    {
 
   }
}
