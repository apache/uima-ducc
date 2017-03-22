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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.cli.UimaAsPing;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.IStateServices.SvcMetaProps;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesFactory;
import org.apache.uima.ducc.common.persistence.services.StateServicesSet;
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
import org.apache.uima.ducc.transport.event.AServiceRequest;
import org.apache.uima.ducc.transport.event.ServiceDisableEvent;
import org.apache.uima.ducc.transport.event.ServiceEnableEvent;
import org.apache.uima.ducc.transport.event.ServiceIgnoreEvent;
import org.apache.uima.ducc.transport.event.ServiceModifyEvent;
import org.apache.uima.ducc.transport.event.ServiceObserveEvent;
import org.apache.uima.ducc.transport.event.ServiceQueryEvent;
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
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.sm.IService.Trinary;
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
	private static DuccLogger logger = DuccLogger.getLogger(ServiceManagerComponent.class.getName(), COMPONENT_NAME);
	private static DuccId jobid = null;

	private DuccWorkMap localMap = null;

    private DuccEventDispatcher eventDispatcher;
    private String stateEndpoint;

    private ServiceHandler handler = null;
    private IStateServices stateHandler = null;

    //HashMap<String, BaseUimaAsService> services = new HashMap<String, BaseUimaAsService>();

    static int meta_ping_rate = 60000;       // interval in ms to ping the service
    static int meta_ping_stability = 5;           // number of missed pings before we mark the service down
    static int meta_ping_timeout = 500;      // timeout on ping
    static String default_ping_class;

    static int init_failure_max = 1;       // total
    static int failure_max = 5;              // total in window
    static int failure_window = 30;          // window size in minutes

    private String state_dir = null;
    private String state_file = null;

    private DuccProperties sm_props = null;
    private String service_seqno = IStateServices.sequenceKey;
    private DuccIdFactory idFactory = new DuccIdFactory();

    private boolean signature_required = true;
    private boolean initialized = false;
    private boolean testmode = false;
    private boolean orchestrator_alive = false;

    private Map<String, String> administrators = new HashMap<String, String>();

    // Local SM version
    //    1.1.0 - reworked SM
    //    1.1.3 - added shutdown hook, pinger last-use, pinger disable autostart
    //    1.1.4 - dynamic mod of all registration parms.  Add debug and max-init-time parms.
    //    1.1.0 - resync with release, sigh.
    //    2.0.0 - Update for new release.
    private String version = "2.1.0";

	public ServiceManagerComponent(CamelContext context)
    {
		super("ServiceManager", context);
        this.localMap = new DuccWorkMap();
        handler = new ServiceHandler(this);
	}

	public DuccLogger getLogger()
	{
		return logger;
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

        // recover the registry
        StateServicesDirectory all = stateHandler.getStateServicesDirectory();
        NavigableSet<Long>     svcs = all.getDescendingKeySet();

        for ( Long l : svcs ) {
            StateServicesSet sss = all.get(l);
            DuccProperties svcprops = sss.get(IStateServices.svc);
            DuccProperties metaprops = sss.get(IStateServices.meta);

            int friendly = 0;
            String uuid = "";
            try {
                // these gets will throw if the requisite objects aren't found
                friendly = metaprops.getIntProperty("numeric_id");
                uuid = metaprops.getStringProperty("uuid");
            } catch (MissingPropertyException e1) {
                // Ugly, but shouldn't have to be fatal
                logger.error(methodName, null, "Cannot restore DuccId for service", l, "Friendly id:", friendly, "uuid:", uuid);
                continue;
            }

            System.out.println("Meta id " + metaprops.get("meta_dbid"));
            System.out.println("Svc id " + metaprops.get("svc_dbid"));
            DuccId id = new DuccId(friendly);
            id.setUUID(UUID.fromString(uuid));
            logger.debug(methodName, id, "Unique:", id.getUnique());

            try {
                handler.register(id, svcprops, metaprops, true);
            } catch (IllegalStateException e ) {                 // happens on duplicate service
                logger.error(methodName, id, e.getMessage());  // message has all I need.
            }

        }

        // try {
		// 	File histdir = new File(serviceHistoryLocation());
		// 	if ( ! histdir.exists() ) {
		// 		histdir.mkdirs();
		// 	}

        //     Map<Long, Properties> sprops = h.getPropertiesForType(DbVertex.Service);
        //     Map<Long, Properties> mprops = h.getPropertiesForType(DbVertex.ServiceMeta);

        //     for ( Long k : sprops.keySet() ) {
        //         DuccProperties svcprops  = (DuccProperties) sprops.get(k);
        //         DuccProperties metaprops = (DuccProperties) mprops.get(k);

        //         String uuid = metaprops.getProperty("uuid");

        //         DuccId id = new DuccId(k);
        //         id.setUUID(UUID.fromString(uuid));
        //         logger.debug(methodName, id, "Unique:", id.getUnique());

        //         try {
        //             handler.register(id, svcprops, metaprops, true);
        //         } catch (IllegalStateException e ) {                 // happens on duplicate service
        //             logger.error(methodName, id, e);  // message has all I need.
        //         }
        //     }

		// } catch (Throwable e) {
        //     // If we get here we aren't startable.
		// 	logger.error(methodName, null, "Cannot initialize service manger: ", e);
		// 	System.exit(1);
		// } finally {
        //     h.close();
        // }

        state_dir = System.getProperty("DUCC_HOME") + "/state";
        state_file = state_dir + "/sm.properties";

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

        long dbSeqNo = ServiceManagerHelper.getLargestServiceSeqNo();
        if(dbSeqNo > seq) {
        	logger.warn(methodName, jobid, "file:"+seq+" "+"db:"+dbSeqNo);
        	seq = (int) dbSeqNo;
        }

        idFactory = new DuccIdFactory(seq);

        synchronized(this) {
            initialized = true;
        }
    }

    // UIMA-4336 Construct the response as a beany thing.
    static ServiceReplyEvent makeResponse(boolean rc, String message, String endpoint, long id)
    {
        ServiceReplyEvent ret = new ServiceReplyEvent();
        ret.setReturnCode(rc);
        ret.setMessage(message);
        ret.setEndpoint(endpoint);
        ret.setId(id);
        return ret;
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

        init_failure_max = SystemPropertyResolver.getIntProperty("ducc.sm.init.failure.limit"         , init_failure_max);
        failure_max      = SystemPropertyResolver.getIntProperty("ducc.sm.instance.failure.limit"     , failure_max);
        failure_window   = SystemPropertyResolver.getIntProperty("ducc.sm.instance.failure.window"    , failure_window);

        meta_ping_rate      = SystemPropertyResolver.getIntProperty("ducc.sm.meta.ping.rate"          , meta_ping_rate);
        meta_ping_timeout   = SystemPropertyResolver.getIntProperty("ducc.sm.meta.ping.timeout"       , meta_ping_timeout);
        meta_ping_stability = SystemPropertyResolver.getIntProperty("ducc.sm.meta.ping.stability"     , meta_ping_stability);
        default_ping_class  = SystemPropertyResolver.getStringProperty("ducc.sm.default.monitor.class", UimaAsPing.class.getName());

        String rm = SystemPropertyResolver.getStringProperty("ducc.runmode", "");
        if ( rm.equals("Test") ) testmode = true;

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
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    JVM                     : ", System.getProperty("java.vendor") +
                                                                   " "+ System.getProperty("java.version"));
        logger.info(methodName, null, "    JAVA_HOME               : ", System.getProperty("java.home"));
        logger.info(methodName, null, "    JVM Path                : ", System.getProperty("ducc.jvm"));
        logger.info(methodName, null, "    JMX URL                 : ", System.getProperty("ducc.jmx.url"));
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    OS Architecture         : ", System.getProperty("os.arch"));
        logger.info(methodName, null, "    Crypto enabled          : ", signature_required);
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    Test mode enabled       : ", testmode);
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    Service ping rate       : ", meta_ping_rate);
        logger.info(methodName, null, "    Service ping timeout    : ", meta_ping_timeout);
        logger.info(methodName, null, "    Service ping stability  : ", meta_ping_stability);
        logger.info(methodName, null, "    Default ping class      : ", default_ping_class);
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    database enabled        : ", !System.getProperty("ducc.database.host").equals("--disabled--"));
        logger.info(methodName, null, "    database implementation : ", System.getProperty("ducc.service.persistence.impl"));
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    Init Failure Max        : ", init_failure_max);
        logger.info(methodName, null, "    Instance Failure Max    : ", failure_max);
        logger.info(methodName, null, "    Instance Failure Window : ", failure_window);
        logger.info(methodName, null, "");
        logger.info(methodName, null, "    DUCC Version            : ", Version.version());
        logger.info(methodName, null, "    SM Version              : ", version);
        logger.info(methodName, null, "------------------------------------------------------------------------------------");

        readAdministrators();

        stateHandler = StateServicesFactory.getInstance(this.getClass().getName(), COMPONENT_NAME);

        // // String dbname = System.getProperty("ducc.db.name");
        // String dburl  = System.getProperty("ducc.state.database.url"); // "remote:localhost:2424/DuccState"

		// try {
        //     // verify, and possibly set up the schema if it's the first time
		// 	databaseHandler = new DbManager(dburl);
        //     databaseHandler.init();
		// } catch (Throwable e) {
        //     logger.fatal(methodName, null, "Cannot create database at", dburl, ":", e);
        //     Runtime.getRuntime().halt(1);
		// }

        // if ( databaseHandler == null ) {
        //     logger.error(methodName, null, "Cannot open database at", dburl);
        // } else {
        //     logger.info(methodName, null, "Opened database at", dburl);
        // }
        handler.setStateHandler(stateHandler);

        // Here is a good place to do any pre-start stuff

        // Start the main processing loop
        Thread smThread = new Thread(this);
        smThread.setName("ServiceManagerHandler");
        smThread.setDaemon(true);
        smThread.start();

        Thread handlerThread = new Thread(handler);
        handlerThread.setName("ServiceHandler");
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

    public boolean isAdministrator(AServiceRequest ev)
    {
        // must be in the list, and have asked nicely as well
        return administrators.containsKey(ev.getUser()) && (ev.asAdministrator());
    }

    /**
     * At boot only ... pass in the set of all known active services to each service so it can update
     * internal state with current published state.
     */
    public synchronized void bootHandler(IDuccWorkMap work)
    {
        Map<DuccId, DuccWorkJob> services = new HashMap<DuccId, DuccWorkJob>();
        for ( Object o : work.values() ) {
        	IDuccWork w = (IDuccWork) o;
            if ( w.getDuccType() != DuccType.Service ) continue;
            DuccWorkJob j = (DuccWorkJob) w;
            if ( !j.isActive() ) continue;
            services.put(j.getDuccId(), j);
        }
        handler.bootImplementors(services);
    }

    void diffCommon(IDuccWork l, IDuccWork r, HashMap<DuccId, IDuccWork> modifiedJobs, HashMap<DuccId, IDuccWork> modifiedServices)
    {
    	String methodName = "diffCommon";
        if ( l.getDuccType() == DuccType.Reservation ) return;

        if ( l.getDuccType() == DuccType.Pop ) {
            logger.trace(methodName, l.getDuccId(), "BOTH: GOT A POP:", l.getDuccId());
        }

        if ( l.getStateObject() != r.getStateObject() ) {
            String serviceType = "/ Job";
            switch ( l.getDuccType() ) {
                case Service:
                case Pop:
                    switch ( ((IDuccWorkService)l).getServiceDeploymentType() )
                        {
                        case uima:
                        case custom:
                            serviceType = "/ Service";
                            break;
                        case other:
                            serviceType = "/ ManagedReservation";
                        default:
                            break;
                        }
                    break;
                default:
                    break;
            }
            logger.trace(methodName, l.getDuccId(), "Reconciling", l.getDuccType(), serviceType, "incoming state = ", l.getStateObject(), " my state = ", r.getStateObject());
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
                    default:
                        break;
                    }
                break;

            default:
                break;
        }
    }


    /**
     * Split the incoming work into new, deleted, and needs update.  This runs under the
     * incoming camel thread so don't do anything timeconsuming here.
     *
     * Also maintain the local workMap so we can diff.
     *
     * Runs on the incoming thread, do not do anything blocking or timecomsuming here.
     */
    public synchronized void processIncoming(IDuccWorkMap workMap)
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


        // try {
        //     ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("/home/challngr/for/jerry/working/incomingWorkMap.obj"));
        //     oos.writeObject(workMap);
        //     oos.close();

        //     oos = new ObjectOutputStream(new FileOutputStream("/home/challngr/for/jerry/working/existingWorkMap.obj"));
        //     oos.writeObject(localMap);
        //     oos.close();
        // } catch ( Throwable t ) {
        //     logger.error(methodName, null, t);
        // }

		@SuppressWarnings("unchecked")
		DuccMapDifference<DuccId, IDuccWork> diffmap = DuccCollectionUtils.difference(workMap, localMap);

        for ( Object o : workMap.values() ) {
        	IDuccWork w = (IDuccWork) o;
            logger.trace(methodName, w.getDuccId(), w.getDuccType(), "Arrives in state =", w.getStateObject());
            // if ( w.getDuccId().getFriendly() == 204 ) {
            // 	int a = 1;
            // 	a++;
            // }
        }

        // Stuff on the left is new
        Map<DuccId, IDuccWork> work = diffmap.getLeft();
        for ( IDuccWork w : work.values() ) {

        	logger.trace(methodName, w.getDuccId(), "Calculating diffs on left side.", w.getDuccId());
            if ( w.getDuccType() == DuccType.Reservation ) continue;

            if ( w.getDuccType() == DuccType.Pop ) {
                logger.trace(methodName, w.getDuccId(), "NEW: GOT A POP:", w.getDuccId());
            }

            if ( !((DuccWorkJob)w).isActive() ) continue;         // not active, we don't care about it. likely after restart.

            logger.trace(methodName, w.getDuccId(), "Reconciling, adding", w.getDuccType());
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
                      default:
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
        	logger.trace(methodName, w.getDuccId(), "Doing diffs on right");
            if ( w.getDuccType() == DuccType.Reservation ) continue;

            if ( w.getDuccType() == DuccType.Pop ) {
                logger.trace(methodName, w.getDuccId(), "DELETED: GOT A POP:", w.getDuccId());
            }

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
                      default:
                          break;
                  }
                  break;

              default:
                  break;
            }
        }

        // NOTE: 2014-07-14 There is some sort of bug in the equals() method on DuccWork so it incorrectly
        //       identifies work as having difference when it doesn't.  As a result this code was originnaly
        //       written under mistaken assumptions of what the map difference returns.  Untkl the owner of
        //       the DuccWork object have worked out a correct equals(), we run the intersection on ALL
        //       intersecting objects, whether they differ in state or do not; hence the two
        //       loops below on the diffmap iterator and on diffmap.getCommon()
        //
        //
        // Now: stuff we both know about. Here is stuff that is in both maps the the map diff identifies as
        // having state differences.
        //
        for( DuccMapValueDifference<IDuccWork> jd: diffmap ) {
            IDuccWork r = jd.getRight();
            IDuccWork l = jd.getLeft();

        	logger.trace(methodName, r.getDuccId(), "Doing diffs on middle A:", r.getDuccId(), l.getDuccId());

            diffCommon(l, r, modifiedJobs, modifiedServices);
        }

        //
        // Common stuff - in both maps the the state diff identifies as haveing no state differences.
        //
        work = diffmap.getCommon();
        for( DuccId k : work.keySet()) {
            IDuccWork r = (IDuccWork) localMap.get(k);
            IDuccWork l = (IDuccWork) workMap.get(k);

         	logger.trace(methodName, r.getDuccId(), "Doing diffs on middle B:", r.getDuccId(), l.getDuccId());

            diffCommon(l, r, modifiedJobs, modifiedServices);
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
    IDuccWorkMap incomingMap = null;
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

    public synchronized void orchestratorStateArrives(IDuccWorkMap map)
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

        orchestrator_alive = true;
        epochCounter++;
        incomingMap = map;
        notify();
    }

    // @deprecated
    static String serviceFileLocation()
    {
        return System.getProperty("DUCC_HOME") + "/state/services";
    }

    static String serviceHistoryLocation()
    {
        return System.getProperty("DUCC_HOME") + "/history/services-registry/";
    }

	  private boolean check_signature(String user, byte[] auth_block)
        throws Throwable
    {
        Crypto crypto = new Crypto(user);
        return crypto.isValid(auth_block);
	  }

    private boolean validate_user(String action, AServiceRequest req)
    {
    	String methodName = "validate_user";

        // First check that request is from a compatible cli
        if (req.getCliVersion() != CliVersion.getVersion()) {
            String reason = "Incompatible CLI request using version " + req.getCliVersion()
                            + " while DUCC expects version " + CliVersion.getVersion();
            logger.warn(methodName, null, action + " rejected. " + reason);
            req.setReply(makeResponse(false, reason, action, -1));
            return false;
        }

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
            req.setReply(makeResponse(false, "User " + user + " cannot be validated. " + action + " rejected.", action, -1));
            return false;
        }
        return true;
    }

    public boolean orchestratorAlive(String action, AServiceRequest req)
    {
    	String methodName = "orchestratorAlive";
        if (  orchestrator_alive ) return true;

        logger.warn(methodName, null, action, "rejected: orchestrator is not yet active");
        req.setReply(makeResponse(false, action + " rejected, DUCC is still initializing.", action, -1));
        return false;
    }

    public synchronized void register(ServiceRegisterEvent ev)
    {
        String methodName = "register";
        DuccProperties props = ev.getDescriptor();
        String endpoint = ev.getEndpoint();
        int instances = ev.getNinstances();
        Trinary autostart = ev.getAutostart();
        String user = ev.getUser();
        long regdate = System.currentTimeMillis();
        String regdate_readable = (new Date(regdate)).toString();

        if ( ! validate_user("Register", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Register", ev) ) return;

        DuccId id = null;
        try {
            id = newId();
        } catch ( Exception e ) {
            logger.error(methodName, null, e);
            ev.setReply(makeResponse(false, "Internal error; unable to generate id", endpoint, -1));
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
        meta.setProperty(SvcMetaProps.user.pname(), user);
        meta.setProperty(SvcMetaProps.instances.pname(), ""+instances);
        meta.setProperty(SvcMetaProps.endpoint.pname(), endpoint);
        meta.setProperty(SvcMetaProps.numeric_id.pname(), id.toString());
        meta.setProperty(SvcMetaProps.uuid.pname(), id.getUnique());
        meta.setProperty(SvcMetaProps.registration_date_millis.pname(), Long.toString(regdate));
        meta.setProperty(SvcMetaProps.registration_date.pname(), regdate_readable);

        if ( autostart == Trinary.True ) {
            meta.setProperty(SvcMetaProps.autostart.pname(), "true");
        } else {
            meta.setProperty(SvcMetaProps.autostart.pname(), "false");
        }

        ServiceReplyEvent reply = handler.register(id, props, meta, false);
        ev.setReply(reply);

        // Draw attention in the log on registration failures
        if ( reply.getReturnCode() ) {
            logger.info(methodName, id, ev.toString());
        } else {
            logger.warn(methodName, id, ev.toString());
        }
    }

    public synchronized void unregister(ServiceUnregisterEvent ev)
    {
        if ( ! validate_user("Unregister", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Unregister", ev) ) return;

        ServiceReplyEvent reply = handler.unregister(ev);
        ev.setReply(reply);
    }

    public synchronized void start(ServiceStartEvent ev)
    {
        String methodName = "start";

        if ( ! validate_user("Start", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Start", ev) ) return;

        logger.info(methodName, null, "Starting service", ev.toString());
        ServiceReplyEvent reply = handler.start(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void stop(ServiceStopEvent ev)
    {
        String methodName = "stop";

        if ( ! validate_user("Stop", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Stop", ev) ) return;

        logger.info(methodName, null, "Stopping service", ev.toString());
        ServiceReplyEvent reply = handler.stop(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void enable(ServiceEnableEvent ev)
    {
        String methodName = "enable";

        if ( ! validate_user("Enable", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Enable", ev) ) return;

        logger.info(methodName, null, "Enabling service", ev.toString());
        ServiceReplyEvent reply = handler.enable(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void disable(ServiceDisableEvent ev)
    {
        String methodName = "disable";

        if ( ! validate_user("Disable", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Disable", ev) ) return;

        logger.info(methodName, null, "Disabling service", ev.toString());
        ServiceReplyEvent reply = handler.disable(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void observe(ServiceObserveEvent ev)
    {
        String methodName = "observe";

        if ( ! validate_user("Observe", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Observe", ev) ) return;

        logger.info(methodName, null, "Observing references for service", ev.toString());
        ServiceReplyEvent reply = handler.observe(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void ignore(ServiceIgnoreEvent ev)
    {
        String methodName = "ignore";

        if ( ! validate_user("Ignore", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Ignore", ev) ) return;

        logger.info(methodName, null, "Ignoring references for service", ev.toString());
        ServiceReplyEvent reply = handler.ignore(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void query(ServiceQueryEvent ev)
    {
        String methodName = "query";

        if ( ! validate_user("Query", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Query", ev) ) return;

        logger.info(methodName, null, "Query", ev.toString());
        ServiceReplyEvent reply = handler.query(ev);
        ev.setReply(reply);
        //ev.setReply(ServiceCode.OK, "Service not implemented.", "no-endpoint", null);
    }

    public synchronized void modify(ServiceModifyEvent ev)
    {
        String methodName = "modify";

        if ( ! validate_user("Modify", ev) ) return;   // necessary messages emitted in here
        if ( ! orchestratorAlive("Modify", ev) ) return;

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
				 job_props.store(fos, "Archived svc properties.");
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
