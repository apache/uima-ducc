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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils.DuccMapDifference;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils.DuccMapValueDifference;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.MissingPropertyException;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.Version;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.ServiceModifyEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryReplyEvent;
import org.apache.uima.ducc.transport.event.ServiceRegisterEvent;
import org.apache.uima.ducc.transport.event.ServiceReplyEvent;
import org.apache.uima.ducc.transport.event.ServiceStartEvent;
import org.apache.uima.ducc.transport.event.ServiceStopEvent;
import org.apache.uima.ducc.transport.event.ServiceUnregisterEvent;
import org.apache.uima.ducc.transport.event.SmStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccState.JobState;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.sm.ServiceMap;


/**
 * This is the logical "main".  The framework instantiates it and calls the (inherited) start() method.
 * Start() establishes the class in its own thread and fires up the Handler thread.  From then on it
 * is a conduit between the Handler and messages to/from the outside world.
 */
public class ServiceManagerComponent 
    extends AbstractDuccComponent 
    implements IServiceManager,
               SmConstants,
               Runnable
{
	
	private static DuccLogger logger = DuccLogger.getLogger(ServiceManagerComponent.class.getName(), COMPONENT_NAME);	
    DuccWorkMap localMap = null;

    private DuccEventDispatcher eventDispatcher;
    private String stateEndpoint;

    private ServiceHandler handler = null;

    //HashMap<String, BaseUimaAsService> services = new HashMap<String, BaseUimaAsService>();	


    static int meta_ping_rate = 60000;       // interval in ms to ping the service
    static int meta_ping_stability = 5;           // number of missed pings before we mark the service down
    static int meta_ping_timeout = 500;      // timeout on ping 
    static String default_ping_class;

    private String state_dir = null;
    private String state_file = null;
    private String descriptor_dir = null;
    private DuccProperties sm_props = null;
    private String service_seqno = "service.seqno";
    private DuccIdFactory idFactory = new DuccIdFactory();

	public ServiceManagerComponent(CamelContext context) 
    {
		super("ServiceManager", context);
        this.localMap = new DuccWorkMap();
        handler = new ServiceHandler(this);
	}


    /**
     * Initialization tasks:
     * - read all the service descriptors
     * - ping them and update their state
     */
    void init()
    	throws Exception
    {
    	String methodName = "init";
        try {
			File descdir = new File(serviceFileLocation());
			if ( ! descdir.exists() ) {
				descdir.mkdirs();
			}
			String[] desclist = descdir.list();
			for ( String d : desclist) {
                if ( d.endsWith(".svc") ) {
                    int ndx = d.lastIndexOf(".");
                    String stem = d.substring(0, ndx);
                    
                    DuccProperties props = new DuccProperties();
                    String props_filename = serviceFileKey(d);
                    props.load(props_filename);

                    
                    DuccProperties metaprops = new DuccProperties();
                    String meta_filename = serviceFileKey(stem + ".meta");
                    metaprops.load(meta_filename);
                    
                    
                    int friendly = 0;
					String uuid = "";
					try {
						// these gets will throw if the requisite objects aren't found
						friendly = metaprops.getIntProperty("numeric_id");
						uuid = metaprops.getStringProperty("uuid");
					} catch (MissingPropertyException e1) {
						// Ugly, but shouldn't have to be fatal
						logger.error(methodName, null, "Cannot restore DuccId for", d, "Friendly id:", friendly, "uuid:", uuid);
						continue;
					}

                    DuccId id = new DuccId(friendly);
                    id.setUUID(UUID.fromString(uuid));
                    logger.debug(methodName, id, "Unique:", id.getUnique());
                    
                    try {
                        handler.register(id, props_filename, meta_filename, props, metaprops);
                    } catch (IllegalStateException e ) {                 // happens on duplicate service
                        logger.error(methodName, id, e.getMessage());  // message has all I need.
                    }
                        
                }
            }

		} catch (Throwable e) {
            // If we get here we aren't startable.
			logger.error(methodName, null, "Cannot initialize service manger: ", e.getMessage());
			System.exit(1);
		}

        state_dir = System.getProperty("DUCC_HOME") + "/state";
        state_file = state_dir + "/sm.properties";
        descriptor_dir = state_dir + "/services";
        File ddir = new File(descriptor_dir);
        if ( ddir.exists() ) {
            if ( ! ddir.isDirectory() ) {
                throw new IllegalStateException("Service descriptor location is not a directory: " + descriptor_dir);
            }
        } else {
            ddir.mkdirs();
        }

        sm_props = new DuccProperties();
        File sf = new File(state_file);
        int seq = 0;
        FileInputStream fos;
        if ( sf.exists() ) {
            fos = new FileInputStream(state_file);
            try {
                sm_props.load(fos);
                String s = sm_props.getProperty(service_seqno);
                seq = Integer.parseInt(s) + 1;
            } finally {
                fos.close();
            }
        } 

        idFactory = new DuccIdFactory(seq);
    }
	
	@Override
	public void start(DuccService service, String[] args) throws Exception 
    {
		String methodName = "start";
		super.start(service, args);
		DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.ServiceManager,getProcessJmxUrl());

        meta_ping_rate = SystemPropertyResolver.getIntProperty("ducc.sm.meta.ping.rate", meta_ping_rate);
        meta_ping_timeout = SystemPropertyResolver.getIntProperty("ducc.sm.meta.ping.timeout", meta_ping_timeout);
        meta_ping_stability = SystemPropertyResolver.getIntProperty("ducc.sm.meta.ping.stability", meta_ping_stability);
        default_ping_class = SystemPropertyResolver.getStringProperty("ducc.sm.default.uima-as.ping.class", UimaAsPing.class.getName());

        logger.info(methodName, null, "------------------------------------------------------------------------------------");
        logger.info(methodName, null, "Service Manager starting:");
        logger.info(methodName, null, "    DUCC home               : ", System.getProperty("DUCC_HOME"));
        logger.info(methodName, null, "    ActiveMQ URL            : ", System.getProperty("ducc.broker.url"));
        logger.info(methodName, null, "    JVM                     : ", System.getProperty("java.vendor") +
                                                                   " "+ System.getProperty("java.version"));
        logger.info(methodName, null, "    JAVA_HOME               : ", System.getProperty("java.home"));
        logger.info(methodName, null, "    JVM Path                : ", System.getProperty("ducc.jvm"));
        logger.info(methodName, null, "    JMX URL                 : ", System.getProperty("ducc.jmx.url"));
        logger.info(methodName, null, "    OS Architecture         : ", System.getProperty("os.arch"));
        logger.info(methodName, null, "    Service ping rate       : ", meta_ping_rate);
        logger.info(methodName, null, "    Service ping timeout    : ", meta_ping_timeout);
        logger.info(methodName, null, "    Service ping stability  : ", meta_ping_stability);
        logger.info(methodName, null, "    DUCC Version            : ", Version.version());
        logger.info(methodName, null, "------------------------------------------------------------------------------------");

        // Here is a good place to do any pre-start stuff

        // Start the main processing loop
        Thread smThread = new Thread(this);
        smThread.setDaemon(true);
        smThread.start();

        Thread handlerThread = new Thread(handler);
        handlerThread.setDaemon(true);
        handlerThread.start();
	}

    public void run()
    {
        String methodName = "run";

    	logger.info(methodName, null, "Starting Service Manager");
        try {
            init();
            runSm();
        } catch ( Throwable t ) {
            logger.error(methodName, null, t);
        }
    	logger.info(methodName, null, "Service Manger returns.");
    }

    /**
     * At boot only ... pass in the set of all known active services to each service so it can update
     * internal state with current published state.
     */
    public synchronized void synchronizeHandler(DuccWorkMap work) 
    {
        Map<DuccId, JobState> ids = new HashMap<DuccId, JobState>();
        for ( IDuccWork w : work.values() ) {
            if ( w.getDuccType() != DuccType.Service ) continue;
            DuccWorkJob j = (DuccWorkJob) w;
            if ( !j.isActive() ) continue;
            ids.put(j.getDuccId(), j.getJobState());
        }
        handler.synchronizeImplementors(ids);
    }
	
    /**
     * Split the incoming work into new, deleted, and needs update.  This runs under the
     * incoming camel thread so don't do anything timeconsuming here.
     *
     * Also maintain the local workMap so we can diff.
     *
     * Runs on the incoming thread, do not do anything blocking or timecomsuming here.
     */
    public synchronized void processIncoming(DuccWorkMap workMap) 
    {
		String methodName = "processIncoming";

        HashMap<DuccId, IDuccWork> newJobs = new HashMap<DuccId, IDuccWork>();
        HashMap<DuccId, IDuccWork> newServices = new HashMap<DuccId, IDuccWork>();

        HashMap<DuccId, IDuccWork> deletedJobs = new HashMap<DuccId, IDuccWork>();
        HashMap<DuccId, IDuccWork> deletedServices = new HashMap<DuccId, IDuccWork>();

        HashMap<DuccId, IDuccWork> modifiedJobs = new HashMap<DuccId, IDuccWork>();
        HashMap<DuccId, IDuccWork> modifiedServices = new HashMap<DuccId, IDuccWork>();

		logger.debug(methodName, null, "---Processing Orchestrator State---");

        if ( workMap.size() == 0 ) {
            logger.debug(methodName, null, "OR state is empty");
            return;
        }

        @SuppressWarnings("unchecked")
		DuccMapDifference<DuccId, IDuccWork> diffmap = DuccCollectionUtils.difference(workMap, localMap);        

        for ( IDuccWork w : workMap.values() ) {
        	//IDuccWork j = (IDuccWork) w;
            logger.debug(methodName, w.getDuccId(), w.getDuccType(), "Arrives in state =", w.getStateObject());
        }

        // Stuff on the left is new
        Map<DuccId, IDuccWork> work = diffmap.getLeft();
        for ( IDuccWork w : work.values() ) {

            if ( w.getDuccType() == DuccType.Reservation ) continue;

            if ( !((DuccWorkJob)w).isActive() ) continue;         // not active, we don't care about it. likely after restart.
            logger.debug(methodName, w.getDuccId(), "Reconciling, adding", w.getDuccType());
			switch(w.getDuccType()) {
              case Job:
                  localMap.addDuccWork(w);
                  newJobs.put(w.getDuccId(), w);
                  break;

              case Service:
                  localMap.addDuccWork(w);
                  // An arbitrary process is **almost** the same as a service in terms of how most of DUCC
                  // handles it.  In order to transparently reuse all that code it is classified as a
                  // special type of service, "other", which the SM treats as a regular job.
                  switch ( ((IDuccWorkService)w).getServiceDeploymentType() ) 
                  {
                      case uima:
                      case custom:
                          newServices.put(w.getDuccId(), w);
                          break;
                      case other:
                          newJobs.put(w.getDuccId(), w);
                          break;
                  }

                  break;

              default:
                  break;
            }
        }

        // Stuff on the left is stuff we have but or doesn't
        work = diffmap.getRight();
        for ( IDuccWork w : work.values() ) {
            if ( w.getDuccType() == DuccType.Reservation ) continue;

            logger.debug(methodName, w.getDuccId(), "Reconciling, deleting", w.getDuccType());
			switch(w.getDuccType()) {
              case Job:
                  localMap.removeDuccWork(w.getDuccId());
                  deletedJobs.put(w.getDuccId(), w);
                  break;

              case Service:
                  localMap.removeDuccWork(w.getDuccId());
                  switch ( ((IDuccWorkService)w).getServiceDeploymentType() ) 
                  {
                      case uima:
                      case custom:
                          deletedServices.put(w.getDuccId(), w);
                          break;
                      case other:
                          deletedJobs.put(w.getDuccId(), w);
                          break;
                  }
                  break;

              default:
                  break;
            }
        }

        // Now: stuff we both know about
        for( DuccMapValueDifference<IDuccWork> jd: diffmap ) {

            IDuccWork r = jd.getRight();
            IDuccWork l = jd.getLeft();

            if ( l.getDuccType() == DuccType.Reservation ) continue;

            logger.debug(methodName, l.getDuccId(), "Reconciling, incoming state = ", l.getStateObject(), " my state = ", r.getStateObject());

            // Update our own state by replacing the old (right) object with the new (left)
			switch(l.getDuccType()) {
              case Job:
                  modifiedJobs.put(l.getDuccId(), l);
                  localMap.addDuccWork(l);
                  break;

              case Service:
                  localMap.addDuccWork(l);
                  switch ( ((IDuccWorkService)l).getServiceDeploymentType() ) 
                  {
                      case uima:
                      case custom:
                          modifiedServices.put(l.getDuccId(), l);
                          break;
                      case other:
                          modifiedJobs.put(l.getDuccId(), l);
                          break;
                  }
                  break;

              default:
                  break;
            }
        }
        handler.signalUpdates(
                              newJobs, 
                              newServices,                               
                              deletedJobs,
                              deletedServices,                                    
                              modifiedJobs,
                              modifiedServices
                              );
	}

    /**
     * Publish the map, called by the ServiceHandler.
     */
    public void publish(ServiceMap map)
    {
        String methodName = "publish";
        try {
            SmStateDuccEvent ev = new SmStateDuccEvent();
            logger.info(methodName, null, "Publishing State, active job count =", map.size());
            if (logger.isDebug()) {
                logger.debug(methodName, null, map.toPrint());
            }
            ev.setServiceMap(map);
            eventDispatcher.dispatch(stateEndpoint, ev, "");  // tell the world what is scheduled (note empty string)
        } catch (Throwable t) {
            logger.error(methodName, null, t);
        }
    }

    /**
    public void processIncomingOld(DuccWorkMap workMap) 
    {
		String methodName = "evaluateServiceRequirements";
		logger.debug(methodName, null, "---Received Orchestrator State---");
		synchronized(serviceMap) {
			jobDisappeared(workMap);
			jobNotActive(workMap);
			servicesUpdate(workMap);
		}
	}
	
	private int jobDisappeared(DuccWorkMap workMap) 
    {
		String methodName = "jobDisappeared";
		int count = 0;
		Iterator<DuccId> serviceMapIterator = serviceMap.keySet().iterator();
		while(serviceMapIterator.hasNext()) {
			DuccId duccId = serviceMapIterator.next();
			if(!workMap.containsKey(duccId)) {
				count++;
				serviceMap.removeService(duccId);
				logger.info(methodName, duccId, "Job removed (disappeared)");
			}
		}
		return count;
	}
	
	private int jobNotActive(DuccWorkMap workMap) 
    {
		String methodName = "jobNotActive";
		int count = 0;
		Iterator<DuccId> workMapIterator = workMap.keySet().iterator();
		while(workMapIterator.hasNext()) {
			DuccId duccId = workMapIterator.next();
			IDuccWork duccWork = workMap.findDuccWork(duccId);
			switch(duccWork.getDuccType()) {
			case Job:
			case Service:
				DuccWorkJob duccWorkJob = (DuccWorkJob) duccWork;
				if(!duccWorkJob.isActive() && serviceMap.containsKey(duccId)) {
					count++;
					serviceMap.removeService(duccId);
					logger.info(methodName, duccId, "Job removed");
				}
				break;
			default:
				break;
			}
		}
		return count;
	}
	
	private int servicesUpdate(DuccWorkMap workMap) 
    {
		String methodName = "servicesUpdate";
		int count = 0;
		Iterator<DuccId> workMapIterator = workMap.keySet().iterator();
		while(workMapIterator.hasNext()) {
			DuccId duccId = workMapIterator.next();
			IDuccWork duccWork = workMap.findDuccWork(duccId);
			switch(duccWork.getDuccType()) 
            {
                case Job:
                case Service:
                    DuccWorkJob job = (DuccWorkJob) duccWork;
                    if ( job.isActive() ) {

                        count++;
                        Services jobServices = new Services();                    
                        if( !serviceMap.containsKey(duccId) ) {
                            serviceMap.addService(duccId, jobServices);
                        }
                        
                        //
                        // TODO: For now this always sets Running
                        //
                        boolean enabled = true;
                        if ( !enabled ) {
                            logger.info(methodName, job.getDuccId(), "Service check bypassed: return ServiceState.Running");
                            jobServices.setState(ServiceState.Running);
                        } else {
                            
                            //   TODO: Service manager will do this once it's ready.
                            //         Will track state of service from OR state as a submitted POP.  Once
                            //         state Running is reached a ping is needed.
                            //   TODO: jobServices needs space for a collection of service states.
                            //
                            if ( dependenciesSatisfied(job) ) {
                                jobServices.setState(ServiceState.Running);
                            } else {
                                jobServices.setState(ServiceState.NotAvailable);
                            }
                        }
                        
                        logger.info(methodName, duccId, "Job updated. State:", jobServices.getState());
                    }
                    break;
    			default:
                    break;
			}
		}
		return count;
	}
*/
//
//	/**
//	 * Returns true if job service dependencies are satisfied. False otherwise.
//	 * 
//	 * @param duccWorkJob - Job with (possible) dependencies on remote services
//	 * @return - true if job's dependencies are satisfied. False otherwise
//	 */
//	private boolean dependenciesSatisfied(DuccWorkJob duccWorkJob) 
//    {
//		String methodName = "dependenciesSatisfied";
//		try {
//            String[] deps = duccWorkJob.getServiceDependencies();
//            if ( deps == null ) {
//                logger.info(methodName, duccWorkJob.getDuccId(), "No service dependencies, returns satisfied.");
//                return true;
//            }
//
//			List<ServiceSpecifier> remoteDependencies = new ArrayList<ServiceSpecifier>();
//            for ( String dep : deps ) {
//                remoteDependencies.add(new ServiceSpecifier(dep));
//			}
//			return validDependencies(duccWorkJob.getDuccId(),remoteDependencies);
//		} catch( Exception e) {
//			logger.error(methodName, duccWorkJob.getDuccId(), e);
//		}
//		return false;
//	}

	/**
	 * Validates availability of remote services. Reads given deployment descriptor,
	 * extracts AE descriptor and parses it to get ResourceSpecifier. The code then 
	 * iterates over delegates looking for CustomResourceSpecifier type. If found, 
	 * a delegate is a remote service which must be tested for availability. An instance
	 * of UIMA AS client is created for each remote service to test GetMeta response. If 
	 * a remote service does not respond, the client times out and service is considered
	 * as not available.
	 * 
	 * @param ddFile - deployment descriptor file
	 * @param duccId - job ducc id
	 * @return - true is dependent services are available. False, otherwise.
	 * @throws Exception
	 */
//	private boolean validDependencies(DuccId duccId, List<ServiceSpecifier> remotes) throws Exception 
//    {
//		String methodName = "validDependencies";
//		boolean answer = true; 
//
//		//	iterate over remotes. Send GetMeta request to each one and wait for reply.
//		//  If the remote is available it will respond. Otherwise, either a
//		//  broker is down or service is not available and we time out.
//		for( ServiceSpecifier remote : remotes ) {
//            BaseUimaAsService sd = services.get(remote.key());
//            if ( sd == null ) {                                         // make sure its registered
//                logger.info(methodName, duccId, "Service not registered:", remote.key());
//                answer = false;
//            } else {
//                boolean p = ping(sd);        // use a tmp so we can log
//                answer &= p;
//                logger.info(methodName, duccId, "Dependency available for", remote.key, ":", p);
//            }
//		} 
//		return answer;
//	}


    public void setTransportConfiguration(DuccEventDispatcher eventDispatcher, String endpoint)
    {
        this.eventDispatcher = eventDispatcher;
        this.stateEndpoint = endpoint;
    }

    int epochCounter = 0;
    DuccWorkMap incomingMap = null;
    public synchronized void runSm()
    {
        String methodName = "runSm";
        boolean first_update = true;

        while ( true ) {

            try {
                wait();
            } catch (InterruptedException e) {
            	logger.info(methodName, null, "SM wait interrupted, executing out-of-band epoch.");
            }
            
            try {
                if ( first_update ) {
                    synchronizeHandler(incomingMap);
                    first_update = false;
                }
                processIncoming(incomingMap);
            } catch (Throwable e1) {
            	logger.fatal(methodName, null, e1);
            }
            
        }
    }

    public synchronized void orchestratorStateArrives(DuccWorkMap map)
    {
        epochCounter++;
        incomingMap = map;
        notify();
    }

    /**
     * Every ping needs to update state if it changes
     */
//    private boolean ping(BaseUimaAsService sd)
//    {
//    	String methodName = "ping";
//        boolean answer = false;
//        ServiceState oldState = sd.getState();
//        answer = sd.ping();
//        if ( sd.getState() != oldState ) {
//        	try {
//        		writeProps(sd);
//        	} catch ( Throwable t ) {
//        		// log the problem but this isn't a good reason to crash anything.
//        		logger.error(methodName, null, "Cannot update state file after ping:", t.getMessage());
//        	}
//        }
//        return answer;
//    }

    private String serviceFileLocation()
    {
        return System.getProperty("DUCC_HOME") + "/state/services";
    }

    private String serviceFileKey(String fn)
    {
        return serviceFileLocation() + "/" + fn;
    }

//    public void writeProps(BaseUimaAsService sd)
//    	throws Throwable
//    {
//        DuccProperties props = sd.getProperties();
//        String fn = serviceFileKey(sd.getStringProperty("service-file-key"));
//        FileOutputStream fos = new FileOutputStream(fn);
//        props.store(fos, "Service Descriptor for " + sd.getKey());
//        fos.close();        
//    }

    public synchronized void register(ServiceRegisterEvent ev)
    {
        String methodName = "register";
        DuccProperties props = ev.getDescriptor();
        String endpoint = ev.getEndpoint();
        int instances = ev.getNinstances();
        Trinary autostart = ev.getAutostart();
        String user = ev.getUser();


        DuccId id = null;
        try {
            id = newId();
        } catch ( Exception e ) {
            logger.error(methodName, null, e);
            ev.setReply(new ServiceReplyEvent(ServiceCode.NOTOK, "Internal error; unable to generate id", endpoint, null));
            return;
        }
        logger.debug(methodName, id, "Unique:", id.getUnique());
                    

        DuccProperties meta = new DuccProperties();
        meta.setProperty("user", user);
        meta.setProperty("instances", ""+instances);
        meta.setProperty("endpoint", endpoint);
        meta.setProperty("numeric_id", id.toString());
        meta.setProperty("uuid", id.getUnique());
        if ( autostart == Trinary.True ) {            
            meta.setProperty("autostart", "true");
        } else {
            meta.setProperty("autostart", "false");
        }

        String desc_name = descriptor_dir + "/" + id + ".svc";
        String meta_name = descriptor_dir + "/" + id + ".meta";
        ServiceReplyEvent reply = handler.register(id, desc_name, meta_name, props, meta);
        ev.setReply(reply);

        // Draw attentipn in the log on registration failures
        switch ( reply.getReturnCode() ) {
            case OK:
                logger.info(methodName, id, ev.toString());
                break;

            case NOTOK:
                logger.warn(methodName, id, ev.toString());
                break;
        }
    }

    public synchronized void unregister(ServiceUnregisterEvent ev)
    {
        String methodName = "unregister";
        long id = ev.getFriendly();
        logger.info(methodName, null, "De-registering service", id);
        ServiceReplyEvent reply = handler.unregister(ev);
        ev.setReply(reply);       
    }

    public synchronized void start(ServiceStartEvent ev)
    {
        String methodName = "startService";
        logger.info(methodName, null, "Starting service", ev.toString());
        ServiceReplyEvent reply = handler.start(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void stop(ServiceStopEvent ev)
    {
        String methodName = "stopService";
        logger.info(methodName, null, "Stopping service", ev.toString());
        ServiceReplyEvent reply = handler.stop(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void query(ServiceQueryEvent ev)
    {
        String methodName = "query";
        logger.info(methodName, null, "Query", ev.toString());
        ServiceQueryReplyEvent reply = handler.query(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void modify(ServiceModifyEvent ev)
    {
        String methodName = "modify";
        logger.info(methodName, null, "Modify", ev.toString());
        ServiceReplyEvent reply = handler.modify(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized DuccId newId()
        throws Exception
    {
        DuccId id = idFactory.next();
        sm_props.setProperty(service_seqno, id.toString());
        FileOutputStream fos = new FileOutputStream(state_file);
        sm_props.store(fos, "Service Manager Properties");
        fos.close();
        return id;
    }

    /**
	public static void main(String[] args) 
    {
		try {
			ServiceManagerComponent sm = new ServiceManagerComponent(new DefaultCamelContext());
			if ( sm.validDependencies( new DuccId(100), new ArrayList<RemoteService>()) ) {
			}

		} catch( Exception e) {
			e.printStackTrace();
		}
	}
	*/
}
