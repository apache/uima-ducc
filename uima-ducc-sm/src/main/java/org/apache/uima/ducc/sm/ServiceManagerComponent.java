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
import java.util.Properties;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.cli.UimaAsPing;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.crypto.Crypto.AccessType;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils.DuccMapDifference;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils.DuccMapValueDifference;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.LinuxUtils;
import org.apache.uima.ducc.common.utils.MissingPropertyException;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.Version;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.AServiceRequest;
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
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
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

    static int failure_max = 5;

    private String state_dir = null;
    private String state_file = null;
    private String descriptor_dir = null;
    private DuccProperties sm_props = null;
    private String service_seqno = "service.seqno";
    private DuccIdFactory idFactory = new DuccIdFactory();
    
    private boolean signature_required = true;
    private boolean initialized = false;
    private boolean testmode = false;

    Map<String, String> administrators = new HashMap<String, String>();
    String version = "1.1.0";

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
			File histdir = new File(serviceHistoryLocation());
			if ( ! histdir.exists() ) {
				histdir.mkdirs();
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

        synchronized(this) {
            initialized = true;
        }
    }

    void readAdministrators()
    {
    	String methodName = "readAdministrators";
        File adminfile = new File(System.getProperty("DUCC_HOME") + "/resources/ducc.administrators");
        if ( ! adminfile.exists() ) {
            logger.info(methodName, null, "No ducc administrators found.");
            return;
        }
        
        Properties props = null;
		try {
			FileInputStream fis = new FileInputStream(adminfile);
			props = new Properties();
			props.load(fis);
		} catch (Exception e) {
            logger.warn(methodName, null, "Cannot read administroators file:", e.toString());
            return;
		}
        
        for ( Object k : props.keySet() ) {
            String adm = ((String) k).trim();
            administrators.put(adm, adm);
            logger.info(methodName, null, "DUCC Administrator registered:", adm);
        }
    }

	@Override
	public void start(DuccService service, String[] args) throws Exception 
    {
		String methodName = "start";
		super.start(service, args);
		DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.ServiceManager,getProcessJmxUrl());

        failure_max = SystemPropertyResolver.getIntProperty("ducc.sm.instance.failure.max", failure_max);
        meta_ping_rate = SystemPropertyResolver.getIntProperty("ducc.sm.meta.ping.rate", meta_ping_rate);
        meta_ping_timeout = SystemPropertyResolver.getIntProperty("ducc.sm.meta.ping.timeout", meta_ping_timeout);
        meta_ping_stability = SystemPropertyResolver.getIntProperty("ducc.sm.meta.ping.stability", meta_ping_stability);
        default_ping_class = SystemPropertyResolver.getStringProperty("ducc.sm.default.monitor.class", UimaAsPing.class.getName());
        String rm = SystemPropertyResolver.getStringProperty("ducc.runmode", "");
        if ( rm.equals("Test") ) testmode = true;

        // yuck
        String sig = SystemPropertyResolver.getStringProperty("ducc.signature.required", "on");
        signature_required = true;
        if      ( sig.equals("on")  ) signature_required = true;
        else if ( sig.equals("off") ) signature_required = false;
        else {
            logger.warn(methodName, null, "Incorrect value for property ducc.signature.required: " + sig + ". Setting to default of \"on\"");
        }

        logger.info(methodName, null, "---------------------------- NEW -----------------------------------------------------");
        logger.info(methodName, null, "Service Manager starting:");
        logger.info(methodName, null, "    DUCC home               : ", System.getProperty("DUCC_HOME"));
        logger.info(methodName, null, "    ActiveMQ URL            : ", System.getProperty("ducc.broker.url"));
        logger.info(methodName, null, "    JVM                     : ", System.getProperty("java.vendor") +
                                                                   " "+ System.getProperty("java.version"));
        logger.info(methodName, null, "    JAVA_HOME               : ", System.getProperty("java.home"));
        logger.info(methodName, null, "    JVM Path                : ", System.getProperty("ducc.jvm"));
        logger.info(methodName, null, "    JMX URL                 : ", System.getProperty("ducc.jmx.url"));
        logger.info(methodName, null, "    OS Architecture         : ", System.getProperty("os.arch"));
        logger.info(methodName, null, "    Crypto enabled          : ", signature_required);
        logger.info(methodName, null, "    Test mode enabled       : ", testmode);
        logger.info(methodName, null, "    Service ping rate       : ", meta_ping_rate);
        logger.info(methodName, null, "    Service ping timeout    : ", meta_ping_timeout);
        logger.info(methodName, null, "    Service ping stability  : ", meta_ping_stability);
        logger.info(methodName, null, "    Instance Failure Max    : ", failure_max);
        logger.info(methodName, null, "    DUCC Version            : ", Version.version());
        logger.info(methodName, null, "    SM Version              : ", version);
        logger.info(methodName, null, "------------------------------------------------------------------------------------");

        readAdministrators();

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

    public boolean isAdministrator(String user)
    {
        return administrators.containsKey(user);
    }

    /**
     * At boot only ... pass in the set of all known active services to each service so it can update
     * internal state with current published state.
     */
    public synchronized void bootHandler(DuccWorkMap work) 
    {
        Map<DuccId, DuccWorkJob> services = new HashMap<DuccId, DuccWorkJob>();
        for ( IDuccWork w : work.values() ) {
            if ( w.getDuccType() != DuccType.Service ) continue;
            DuccWorkJob j = (DuccWorkJob) w;
            if ( !j.isActive() ) continue;
            services.put(j.getDuccId(), j);
        }
        handler.bootImplementors(services);
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

		logger.info(methodName, null, "===== Orchestrator State Arrives =====");

        if ( workMap.size() == 0 ) {
            logger.debug(methodName, null, "OR state is empty");
            return;
        }

        @SuppressWarnings("unchecked")
		DuccMapDifference<DuccId, IDuccWork> diffmap = DuccCollectionUtils.difference(workMap, localMap);        

        for ( IDuccWork w : workMap.values() ) {
            logger.trace(methodName, w.getDuccId(), w.getDuccType(), "Arrives in state =", w.getStateObject());
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
                  // handles it.  To me (SM), however, it is just like any other job so it goes into
                  // the job map.
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

        // Stuff on the right is stuff we have but OR doesn't
        work = diffmap.getRight();
        for ( IDuccWork w : work.values() ) {
            if ( w.getDuccType() == DuccType.Reservation ) continue;

            logger.debug(methodName, w.getDuccId(), "Reconciling, deleting instance of type ", w.getDuccType());
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

            if ( l.getStateObject() != r.getStateObject() ) {
                String serviceType = "/ Job";
                switch ( l.getDuccType() ) {
                  case Service:
                      switch ( ((IDuccWorkService)l).getServiceDeploymentType() ) 
                          {
                          case uima:
                          case custom:
                              serviceType = "/ Service";
                              break;
                          case other:
                              serviceType = "/ ManagedReservation";
                              break;
                          }
                      break;
                      
                }
                logger.debug(methodName, l.getDuccId(), "Reconciling", l.getDuccType(), serviceType, "incoming state = ", l.getStateObject(), " my state = ", r.getStateObject());
            }

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
            logger.trace(methodName, null, "Publishing State, active job count =", map.size());
            if (logger.isDebug()) {
                logger.info(methodName, null, map.toPrint());
            }
            ev.setServiceMap(map);
            eventDispatcher.dispatch(stateEndpoint, ev, "");  // tell the world what is scheduled (note empty string)
        } catch (Throwable t) {
            logger.error(methodName, null, t);
        }
    }

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
                    bootHandler(incomingMap);
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
    	String methodName = "orchestratorStateArrives";
        if ( ! initialized ) {
            logger.info(methodName, null, "SM not initialized, ignoring Orchestrator state update.");
            return;
        }

        if ( ! map.isJobDriverNodeAssigned() ) {
            logger.info(methodName, null, "Orchestrator JD node not assigned, ignoring Orchestrator state update.");
            return;
        }

        epochCounter++;
        incomingMap = map;
        notify();
    }

    static String serviceFileLocation()
    {
        return System.getProperty("DUCC_HOME") + "/state/services";
    }

    static String serviceHistoryLocation()
    {
        return System.getProperty("DUCC_HOME") + "/history/services-registry/";
    }

    private String serviceFileKey(String fn)
    {
        return serviceFileLocation() + "/" + fn;
    }

	private boolean check_signature(String user, byte[] auth_block)
        throws Throwable
    {
        String userHome = null;
        if ( testmode ) {    
            userHome = System.getProperty("user.home");
        } else {
            userHome = LinuxUtils.getUserHome(user);
        }
        
        Crypto crypto = new Crypto(user, userHome,AccessType.READER);
        String signature = (String)crypto.decrypt(auth_block);
        
        return user.equals(signature);
	}

    private boolean validate_user(String action, AServiceRequest req)
    {
    	String methodName = "validate_user";
        String user = req.getUser();                
        byte[] auth_block= req.getAuth();
        boolean validated = false;

        if ( ! signature_required ) return true;

        try {
            validated = check_signature(user, auth_block);
        } catch ( Throwable t ) {
            logger.error(methodName, null, "Crypto failure:", t.toString());
        }

        if ( ! validated ) {
            logger.warn(methodName, null, "User", user, "cannot be validated.", action, "rejected.");
            req.setReply(new ServiceReplyEvent(false, "User " + user + " cannot be validated. " + action + " rejected.", "NONE", -1));
            return false;
        }
        return true;
    }

    public synchronized void register(ServiceRegisterEvent ev)
    {
        String methodName = "register";
        DuccProperties props = ev.getDescriptor();
        String endpoint = ev.getEndpoint();
        int instances = ev.getNinstances();
        Trinary autostart = ev.getAutostart();
        String user = ev.getUser();        
        
        if ( ! validate_user("Register", ev) ) return;   // necessary messages emitted in here

        DuccId id = null;
        try {
            id = newId();
        } catch ( Exception e ) {
            logger.error(methodName, null, e);
            ev.setReply(new ServiceReplyEvent(false, "Internal error; unable to generate id", endpoint, -1));
            return;
        }
        logger.debug(methodName, id, "Unique:", id.getUnique());
                    
        String logdir = props.getProperty(UiOption.LogDirectory.pname());
        if ( !logdir.endsWith("/") ) {
            logdir = logdir + "/";
        } 
        logdir = logdir + "S-" + id.toString();
        props.put(UiOption.LogDirectory.pname(), logdir);

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
        if ( reply.getReturnCode() ) {
            logger.info(methodName, id, ev.toString());
        } else {
            logger.warn(methodName, id, ev.toString());
        }
    }

    public synchronized void unregister(ServiceUnregisterEvent ev)
    {
        String methodName = "unregister";
        long id = ev.getFriendly();

        if ( ! validate_user("Unregister", ev) ) return;   // necessary messages emitted in here

        logger.info(methodName, null, "De-registering service", id);
        ServiceReplyEvent reply = handler.unregister(ev);
        ev.setReply(reply);       
    }

    public synchronized void start(ServiceStartEvent ev)
    {
        String methodName = "startService";

        if ( ! validate_user("Start", ev) ) return;   // necessary messages emitted in here

        logger.info(methodName, null, "Starting service", ev.toString());
        ServiceReplyEvent reply = handler.start(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void stop(ServiceStopEvent ev)
    {
        String methodName = "stopService";

        if ( ! validate_user("Stop", ev) ) return;   // necessary messages emitted in here

        logger.info(methodName, null, "Stopping service", ev.toString());
        ServiceReplyEvent reply = handler.stop(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void query(ServiceQueryEvent ev)
    {
        String methodName = "query";

        if ( ! validate_user("Query", ev) ) return;   // necessary messages emitted in here

        logger.info(methodName, null, "Query", ev.toString());
        ServiceQueryReplyEvent reply = handler.query(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void modify(ServiceModifyEvent ev)
    {
        String methodName = "modify";

        if ( ! validate_user("Modify", ev) ) return;   // necessary messages emitted in here

        logger.info(methodName, null, "Modify", ev.toString());
        ServiceReplyEvent reply = handler.modify(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    Object idSync = new Object();
    public DuccId newId()
        throws Exception
    {
    	DuccId id = null;
        synchronized(idSync) {
            id = idFactory.next();
            sm_props.setProperty(service_seqno, id.toString());
            FileOutputStream fos = new FileOutputStream(state_file);
            sm_props.store(fos, "Service Manager Properties");
            fos.close();
        }
        return id;
    }


    static void deleteProperties(String id, String meta_filename, Properties meta_props, String props_filename, Properties job_props)
    {
        // NOTE: During init we may now know the ID as a DuccId so it has to be passed in as a string

    	String methodName = "deleteProperties";
        // Save a copy in history, and then delete the original
        String history_dir = serviceHistoryLocation();
        if ( meta_filename != null ) {
            File mfh = new File(history_dir + id + ".meta");
			try {
				FileOutputStream fos = new FileOutputStream(mfh);
				meta_props.store(fos, "Archived meta descriptor");            
				fos.close();
			} catch (Exception e) {
				logger.warn(methodName, null, id + ": Unable to save history to \"" + mfh.toString(), ": ", e.toString() + "\"");
			}

            File mf = new File(meta_filename);
            mf.delete();
         }
        meta_filename = null;

         if ( props_filename != null ) {
             File pfh = new File(history_dir + id + ".svc");
             try {
				FileOutputStream fos = new FileOutputStream(pfh);
				 job_props.store(fos, "Archived meta descriptor");            
				 fos.close();
			} catch (Exception e) {
                 logger.warn(methodName, null, id + ":Unable to save history to \"" + pfh.toString(), ": ", e.toString() + "\"");
			}

             File pf = new File(props_filename);
             pf.delete();
         }
         props_filename = null;
    }

}
