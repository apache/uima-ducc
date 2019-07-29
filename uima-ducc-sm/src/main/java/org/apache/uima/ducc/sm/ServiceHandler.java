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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.uima.ducc.cli.DuccServiceApi;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.IStateServices.AccessMode;
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
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
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
    private DuccId jobid = null;
	private IServiceManager serviceManager;

    private ServiceStateHandler serviceStateHandler = new ServiceStateHandler();
	private ServiceMap serviceMap = new ServiceMap();       // note this is the sync object for publish

    private IStateServices stateHandler;

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

    /*
     * initialize data structures (in preparation for resume)
     */
    public void init() {
    	String methodName = "init";
    	logger.debug(methodName, jobid, "");
    	serviceStateHandler = new ServiceStateHandler();
    	serviceMap.clear();  
    }
    
    /*
     * resume: this head node has become master
     */
    public void resume(IDuccWorkMap dwm) {
    	String methodName = "resume";
    	logger.debug(methodName, jobid, "");
    	// clear state
        newJobs.clear();
        newServices.clear();
        deletedJobs.clear();
        deletedServices.clear();
        modifiedJobs.clear();
        modifiedServices.clear();
        pendingRequests.clear();
        // Add implementors from OR publication to SM state
        Set<DuccId> serviceKeys = dwm.getServiceKeySet();
        if(serviceKeys != null) {
        	for(DuccId duccId : serviceKeys) {
            	IDuccWork dw = dwm.findDuccWork(duccId);
            	if(dw != null) {
            		IDuccWorkService service = (IDuccWorkService) dw;
            		long swId = duccId.getFriendly();
            		String endpoint = service.getServiceEndpoint();
            		if(endpoint != null) {
            			ServiceSet ss = serviceStateHandler.getServiceByUrl(endpoint);
            			if(ss != null) {
            				ServiceInstance si = new ServiceInstance(ss);
            				ss.implementors.put(swId, si);
            				addInstance(ss, si);
            				logger.info(methodName, ss.getId(), swId, ss.endpoint);
            			}
            			else {
            				logger.warn(methodName, duccId, "ss == null");
            			}
            		}
            		else {
            			logger.warn(methodName, duccId, "endpoint == null");
            		}
            	}
            	else {
            		logger.warn(methodName, duccId, "dw == null");
            	}
            }
        }
        else {
        	logger.debug(methodName, jobid, "serviceKeys == null");
        }
    }
    
    /*
     * quiesce: this head node has become backup
     */
    public void quiesce() {
    	String methodName = "quiesce";
    	List<ServiceSet> list = serviceStateHandler.getServices();
    	logger.debug(methodName, jobid, list.size());
    	// stop all pingers
        for(ServiceSet ss : list) {
        	DuccId duccId = ss.getId();
        	ss.setState(ServiceState.Dispossessed);
        	logger.info(methodName, duccId, ss.getState(), ss.getImplementors().length, ss.getKey());
        	ss.stopPingThread();
        }
    }
    
    public void setAccessMode(AccessMode accessMode) {
    	stateHandler.setAccessMode(accessMode);
    }
    
    void setStateHandler(IStateServices handler)
    {
        this.stateHandler = handler;
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
    	String methodName = "bootImplementors";
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
            try {
                sset.bootComplete();
            } catch ( Exception e ) {
                logger.warn(methodName, sset.getId(), "Error updating meta properties:", e);
            }
            if ( sset.countImplementors() > 0 ) {            // if something was running, let's make sure all the starts are done
                sset.start();
            }
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
                logger.info(methodName, jobid, "deletedJobsMap", deletedJobsMap.size());

                modifiedJobsMap.putAll(modifiedJobs);
                modifiedJobs.clear();
                logger.info(methodName, jobid, "modifiedJobsMap", modifiedJobsMap.size());
                
                deletedServicesMap.putAll(deletedServices);
                deletedServices.clear();
                logger.info(methodName, jobid, "deletedServicessMap", deletedServicesMap.size());
                
                modifiedServicesMap.putAll(modifiedServices);
                modifiedServices.clear();
                logger.info(methodName, jobid, "modifiedServicesMap", modifiedServicesMap.size());

                newServicesMap.putAll(newServices);
                newServices.clear();
                logger.info(methodName, jobid, "newServicesMap", newServicesMap.size());
                
                newJobsMap.putAll(newJobs);
                newJobs.clear();
                logger.info(methodName, jobid, "newJobsMap", newJobsMap.size());
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
     * It is called only when a new job or service is first discovered in the OR map.
     */
    protected Map<String, ServiceSet> resolveDependencies(DuccWorkJob w, ServiceDependency s)
    {
    	String methodName = "resolveDependencies";
    	DuccId id = w.getDuccId();
        String[] deps = w.getServiceDependencies();
        
        logger.debug(methodName, id, deps.length);

        // New services, if any are discovered
        // Put them into the global map of known services if needed and up the ref count
        boolean fatal = false;
        Map<String, ServiceSet> jobServices = new HashMap<String, ServiceSet>();
        for ( String dep : deps ) {
            ServiceSet sset = serviceStateHandler.getServiceByUrl(dep);
            if ( sset == null ) {
            	logger.debug(methodName, id, dep, "Service is unknown");
                s.addMessage(dep, "Service is unknown.");
                s.setState(ServiceState.NotAvailable);
                fatal = true;
                continue;
            }
            jobServices.put(dep, sset);
        }

        if ( fatal ) {
            jobServices.clear();
        } else {
            for ( ServiceSet sset : jobServices.values() ) {
                // If service is unregistered and then re-registered while the job is running it may have lost
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

        if ( state.ordinality() < 5 ) {       // UIMA-4223, if we got this far, the services all exist but at least one of them
                                              // is not usable.  We use this slightly artificial state to insure the OR keeps
                                              // the work WaitingForServices.
            state = ServiceState.Pending;
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
                    node = ni.getCanonicalName();
                    share_id = pid.getFriendly();
                }
            }

            // Use the service id in case the url has been removed while unregistering
            String ssid = w.getServiceId();
            if (ssid == null ) {              // probably impossible but lets not chance NPE
                logger.warn(methodName, id, "Missing service id, ignoring.");
                continue;
            }

            ServiceSet sset = serviceStateHandler.getServiceByImplementor(id.getFriendly());
            if (sset == null) {
                // If already removed try via the service id (the url may have been removed while unregistering)
                long sid = Long.valueOf(ssid);
                sset = serviceStateHandler.getServiceById(sid);
                if ( sset == null ) {
                    // leftover junk publication maybe? can't tell
                    logger.info(methodName, id, "Active service instance update for", w.getServiceId(),
                                "but have no registration for it. Job state:", w.getJobState());
                    continue;
                }
                logger.info(methodName, id, "Update for possibly unregistered service. Job State:", w.getJobState());
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

    synchronized ServiceReplyEvent query(ServiceQueryEvent ev)      // UIMA-4336 Redeclare return type
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
                reply.setReturnCode(true);
            }
        } else {
            ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
            reply.setEndpoint(url);
            reply.setId(id);
            if ( sset == null ) {
                reply.setMessage("Unknown service");
                reply.setEndpoint(url);
                reply.setReturnCode(false);
            } else {
                IServiceDescription sd = sset.query();
                updateServiceQuery(sd, sset);
                reply.addService(sd);
                reply.setReturnCode(true);
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

    synchronized ServiceReplyEvent start(ServiceStartEvent ev)
    {
        // String methodName = "start";

        long   id  = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return ServiceManagerComponent.makeResponse(false, "Unknown service", url, id);
        }

        if ( ! authorized("start", sset, ev) ) {
            return ServiceManagerComponent.makeResponse(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }


        int running    = sset.countImplementors();
        int instances  = ev.getInstances();

        if ( (instances == -1) && !sset.enabled() ) {    // no args always enables
            sset.enable();
        } else if ( ! sset.enabled() ) {
            return ServiceManagerComponent.makeResponse(false, "Service is disabled, cannot start (" + sset.getDisableReason() + ")", url, sset.getId().getFriendly());
        }

        if ( sset.isDebug() ) {
            if ( sset.countImplementors() > 0 ) {
                return ServiceManagerComponent.makeResponse(true,
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
            return ServiceManagerComponent.makeResponse(true,
                                         "Already has instances[" + running + "] - no additional instances started",
                                         sset.getKey(),
                                         sset.getId().getFriendly());
        }

        pendingRequests.add(new ApiHandler(ev, this));

        if ( sset.isDebug() && (wanted > 1) ) {
            return ServiceManagerComponent.makeResponse(true,
                                         "Instances adjusted to [1] because process_debug is set",
                                         sset.getKey(),
                                         sset.getId().getFriendly());
        } else {
            return ServiceManagerComponent.makeResponse(true,
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

    synchronized ServiceReplyEvent stop(ServiceStopEvent ev)
    {
        String methodName = "stop";

        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return ServiceManagerComponent.makeResponse(false, "Unknown service", url, id);
        }

        if ( ! authorized("stop", sset, ev) ) {
            return ServiceManagerComponent.makeResponse(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( sset.isStopped() ) {
            return ServiceManagerComponent.makeResponse(false, "Already stopped", sset.getKey(), sset.getId().getFriendly());
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
        return ServiceManagerComponent.makeResponse(true, msg, sset.getKey(), sset.getId().getFriendly());
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
            sset.disableAndStop("Disabled by stop from id " + event.getUser());
        } else {
            tolose = Math.min(instances, running);
            sset.updateInstances(Math.max(0, running - tolose)); // pass in target intances running
        }

    }

    synchronized ServiceReplyEvent disable(ServiceDisableEvent ev)
    {
    	String methodName = "disable";
        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return ServiceManagerComponent.makeResponse(false, "Unknown service", url, id);
        }

        if ( ! authorized("disable", sset, ev) ) {
            return ServiceManagerComponent.makeResponse(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( !sset.enabled() ) {
            return ServiceManagerComponent.makeResponse(true, "Service is already disabled", sset.getKey(), sset.getId().getFriendly());
        }

        sset.disable("Disabled by owner or administrator " + ev.getUser());
        try {
            sset.updateMetaProperties();
        } catch ( Exception e ) {
            logger.warn(methodName, sset.getId(), "Error updating meta properties:", e);
        }

        return ServiceManagerComponent.makeResponse(true, "Disabled", sset.getKey(), sset.getId().getFriendly());
    }

    synchronized ServiceReplyEvent enable(ServiceEnableEvent ev)
    {
    	String methodName = "enable";
        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return ServiceManagerComponent.makeResponse(false, "Unknown service", url, id);
        }

        if ( ! authorized("enable", sset, ev) ) {
            return ServiceManagerComponent.makeResponse(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( sset.enabled() ) {
            return ServiceManagerComponent.makeResponse(true, "Service is already enabled", sset.getKey(), sset.getId().getFriendly());
        }

        sset.enable();
        try {
            sset.updateMetaProperties();
        } catch ( Exception e ) {
            logger.warn(methodName, sset.getId(), "Error updating meta properties:", e);
        }
        return ServiceManagerComponent.makeResponse(true, "Enabled.", sset.getKey(), sset.getId().getFriendly());
    }


    synchronized ServiceReplyEvent ignore(ServiceIgnoreEvent ev)
    {
        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return ServiceManagerComponent.makeResponse(false, "Unknown service", url, id);
        }

        if ( ! authorized("ignore", sset, ev) ) {
            return ServiceManagerComponent.makeResponse(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( sset.isAutostart() ) {
            return ServiceManagerComponent.makeResponse(false, "Service is autostarted, ignore-references not applied.", sset.getKey(), sset.getId().getFriendly());
        }

        if ( !sset.isReferencedStart() ) {
            return ServiceManagerComponent.makeResponse(true, "Service is already ignoring references", sset.getKey(), sset.getId().getFriendly());
        }

        if ( sset.countImplementors() == 0 ) {
            return ServiceManagerComponent.makeResponse(false, "Cannot ignore references, service is not running.", sset.getKey(), sset.getId().getFriendly());
        }

        sset.ignoreReferences();
        return ServiceManagerComponent.makeResponse(true, "References now being ignored.", sset.getKey(), sset.getId().getFriendly());
    }

    synchronized ServiceReplyEvent observe(ServiceObserveEvent ev)
    {
        long   id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return ServiceManagerComponent.makeResponse(false, "Unknown service", url, id);
        }

        if ( ! authorized("observe", sset, ev) ) {
            return ServiceManagerComponent.makeResponse(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        if ( sset.isAutostart() ) {
            return ServiceManagerComponent.makeResponse(false, "Must set autostart off before enabling reference-starts.", sset.getKey(), sset.getId().getFriendly());
        }

        if ( sset.countImplementors() == 0 ) {
            return ServiceManagerComponent.makeResponse(false, "Cannot observe references, service is not running.", sset.getKey(), sset.getId().getFriendly());
        }

        sset.observeReferences();
        return ServiceManagerComponent.makeResponse(true, "Observing references.", sset.getKey(), sset.getId().getFriendly());
    }

    synchronized ServiceReplyEvent register(DuccId id, DuccProperties props, DuccProperties meta, boolean isRecovered)
    {
    	String methodName = "register";

        String error;
        boolean must_deregister = false;

        String url = meta.getProperty("endpoint");
        ServiceSet sset = serviceStateHandler.getServiceByUrl(url);
        if (sset != null ) {
            error = "Duplicate registered by " + sset.getUser();
            return ServiceManagerComponent.makeResponse(false, error, url, sset.getId().getFriendly());
        }

        try {
            sset = new ServiceSet(this, this.stateHandler, id, props, meta);
        } catch (Throwable t) {
            // throws because endpoint is not parsable
            error = t.getMessage();
            return ServiceManagerComponent.makeResponse(false, error, url, id.getFriendly());
        }

        try {
            // if it's a "fresh" reservation it must go into the db.  otherwise it is already
            // in the db and doesn't need to be inserted
            sset.storeProperties(isRecovered);
        } catch ( Exception e ) {
            error = ("Internal error; unable to store service descriptor. " + url);
            logger.error(methodName, id, e);
            return ServiceManagerComponent.makeResponse(false, error, url, id.getFriendly());
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

        serviceStateHandler.registerService(id.getFriendly(), url, sset);
        
        // Include warning of class change if any
        String msg = "Registered" + sset.getWarning();
        return ServiceManagerComponent.makeResponse(true, msg, url, id.getFriendly());
    }

    synchronized ServiceReplyEvent modify(ServiceModifyEvent ev)
    {
        long  id   = ev.getFriendly();
        String url = ev.getEndpoint();
    	ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            return ServiceManagerComponent.makeResponse(false, "Unknown service", url, id);
        }

        if ( ! authorized("modify", sset, ev) ) {
            return ServiceManagerComponent.makeResponse(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        pendingRequests.add(new ApiHandler(ev, this));
        return ServiceManagerComponent.makeResponse(true, "Modify accepted:", sset.getKey(), sset.getId().getFriendly());
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
			default:
				// In case a deprecated option such as classpath_order slips through
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
        boolean updateMeta = false;
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
                case Autostart:
                    updateMeta = true;           // UIMA-4928 (Should move it to the svc props)
                default:
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
        try {
            sset.updateSvcProperties();
            if (updateMeta) {
                sset.updateMetaProperties();
            }
        } catch (Exception e) {
            logger.error(methodName, sset.getId(), "Cannot store properties:", e);
        }

        if ( restart_pinger ) {
            sset.restartPinger();
            restart_pinger = false;
        }

        // restart_service - not yet
    }

    synchronized ServiceReplyEvent unregister(ServiceUnregisterEvent ev)
    {
        String methodName = "unregister";
        long id = ev.getFriendly();
        String url = ev.getEndpoint();
        ServiceSet sset = serviceStateHandler.getServiceForApi(id, url);
        if ( sset == null ) {
            logger.info(methodName, null, "Unknown service", id, url);
            return ServiceManagerComponent.makeResponse(false, "Unknown service",  url, id);
        }

        if ( ! authorized("unregister", sset, ev) ) {
            return ServiceManagerComponent.makeResponse(false, "Owned by " + sset.getUser(),  url, sset.getId().getFriendly());
        }

        // Ensure that the event has the id as the name will be removed from the name->id map
        ev.setFriendly(sset.getId().getFriendly());
        serviceStateHandler.unregister(sset);
        pendingRequests.add(new ApiHandler(ev, this));
        logger.info(methodName, null, "Unregistering service", id, url);

        return ServiceManagerComponent.makeResponse(true, "Shutting down implementors", sset.getKey(), sset.getId().getFriendly());
    }

    //
    // Everything to do this must be vetted before it is called. Run in a new thread to not hold up the API.
    //
    void doUnregister(ServiceUnregisterEvent ev)
    {
    	String methodName = "doUnregister";
        long friendly = ev.getFriendly();

        // Can only get by id when unregistering as the name has been removed from the name=>id map
        ServiceSet sset = serviceStateHandler.getServiceById(friendly);
        if ( sset == null ) {  // Should never happen
            logger.error(methodName, null, "Service", friendly, "is not a known, service. No action taken.");
            return;
        }

        String url = sset.getKey();
        sset.disableAndStop("Disabled by unregister from id " + ev.getUser());
        if ( sset.isPingOnly() ) {
            logger.info(methodName, sset.getId(), "Unregister ping-only setvice:", friendly, url);
            serviceStateHandler.removeService(sset);
            try {
				sset.deleteProperties();
			} catch (Exception e) {
				logger.error(methodName, sset.getId(), "Cannot delete service from DB:", e);
			}
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
        serviceStateHandler.removeService(sset);
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
        	System.out.println("Setting shutdown hook");
        }

        public void run()
        {
            System.out.println("Running shutdown hook");
            List<ServiceSet> allServices = serviceStateHandler.getServices();
            for (ServiceSet sset : allServices) {
                sset.stopMonitor();
            }
            try {
                stateHandler.shutdown();
            } catch ( Exception e ) {
            	logger.warn("ServicShutdown.run", null, "Error closing database: ", e);
            }
        }

    }

     class ServiceStateHandler
     {

         // Map of active service descriptors by endpoint.  For UIMA services, key is the endpoint.
         // Map from name->id and from id->service so can quickly unregister by removing from the 1st map UIMA-5372
         private Map<String,  Long>        registeredServiceIdsByUrl       = new HashMap<String,  Long>();
         private Map<Long,    ServiceSet>  registeredServicesById          = new HashMap<Long,    ServiceSet>();

         // Map from instance-id -> service (each instance has a unique ID)
         private Map<Long,    ServiceSet>  servicesByImplementor           = new HashMap<Long, ServiceSet>();

//         // For each job, the collection of services it is dependent upon
//         // DUccId is a Job Id (or id for service that has dependencies)
         private Map<DuccId, Map<Long, ServiceSet>>  servicesByJob = new HashMap<DuccId, Map<Long, ServiceSet>>();

         /*
          * Simply remove the name from the name->id map so the name can be re-used quickly.
          * The now-orphaned id will be used for the remainder of the shutdown steps. UIMA-5372
          */
         synchronized void unregister(ServiceSet sset)
         {
        	 String methodName = "ServiceStateHandler.unregister";
             String key = sset.getKey();
             logger.info(methodName, sset.getId(), "Removing", key, "from name->id map");
             registeredServiceIdsByUrl.remove(key);
             sset.deregister();          // just sets a flag so we know how to handle it when it starts to die
         }

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

             registeredServiceIdsByUrl.put(ep, id);
             registeredServicesById.put(id, sset);
         }

         // Must map url->id then id->service
         synchronized ServiceSet getServiceByUrl(String n)
         {
             Long id = registeredServiceIdsByUrl.get(n);
             return id == null ? null : registeredServicesById.get(id);
         }

         synchronized ServiceSet getServiceById(long id)
         {
             return registeredServicesById.get(id);
         }

         // Note: must exclude services being unregistered
         synchronized List<ServiceSet> getServices()
         {
             ArrayList<ServiceSet> answer = new ArrayList<ServiceSet>();
             for ( ServiceSet sset : registeredServicesById.values() ) {
                 if (!sset.isDeregistered()) {
                     answer.add(sset);
                 }
             }
             return answer;
         }

         synchronized void addImplementorFor(ServiceSet sset, ServiceInstance inst)
         {
             servicesByImplementor.put(inst.getId(), sset);
         }

         synchronized ServiceSet getServiceByImplementor(long instId)
         {
             return servicesByImplementor.get(instId);
         }

         synchronized void removeImplementorFor(ServiceSet sset, ServiceInstance inst)
         {
             servicesByImplementor.remove(inst.getId());
         }

         // API passes in either an id or an endpoint but not both.
         synchronized ServiceSet  getServiceForApi(long id, String n)
         {
             return n == null ? getServiceById(id) : getServiceByUrl(n);
         }

         synchronized void removeService(ServiceSet sset)
         {
             long id = sset.getId().getFriendly();
             registeredServicesById.remove(id);   // The name has already been removed

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
