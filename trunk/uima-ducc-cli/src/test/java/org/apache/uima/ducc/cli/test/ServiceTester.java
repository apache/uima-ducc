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

package org.apache.uima.ducc.cli.test;

import java.io.FileInputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.ducc.cli.DuccJobSubmit;
import org.apache.uima.ducc.cli.DuccManagedReservationCancel;
import org.apache.uima.ducc.cli.DuccManagedReservationSubmit;
import org.apache.uima.ducc.cli.DuccServiceApi;
import org.apache.uima.ducc.cli.IDuccCallback;
import org.apache.uima.ducc.common.IServiceStatistics;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.transport.event.sm.IServiceDescription;
import org.apache.uima.ducc.transport.event.sm.IServiceReply;


// things to do here ...
//    register a service
//    unrgister a service
//    query a service
//    modify a service
//    start
//    stop
//    observe
//    ignore
//    enable
//    disable

public class ServiceTester
{
    DuccProperties ducc_properties;;
    String ducc_home;
    
    String testId;
    String[] service_props;
    String endpoint;
    String service_id;
    ApiRunner job;

    List<String> failReasons = new ArrayList<String>();
    List<String> successReasons = new ArrayList<String>();

    int default_timeout = 20;    // how many polls to allow before calling timeout
    ServiceTester()
    {
    }

    void fail(String testid, String subtestId, String reason)
        throws FailedTestException
    {
        failReasons.add("FAILURE " + testid + " - " + subtestId + " - " + reason);
        throw new FailedTestException(testid + " - " + subtestId + " - test failed: " + reason);
    }

//     void fail(String testid, String reason, Exception e)
//         throws FailedTestException
//     {
//         failReasons.add(testid + ":" + reason);
//         throw new FailedTestException(testid + " test failed: " + reason, e);
//     }

    void success(String testid, String subtestId, String reason)
    {
        successReasons.add("SUCCESS " + testid + " - " + subtestId + " - " + reason);
        System.out.println(testid + " - " + subtestId + " - test passed: " + reason);
    }

    /**
     * Must look into ducc.properties to find the broker.
     */
    String mkBrokerString(String ducc_home)
    	throws Exception
    {
        String proto = ducc_properties.getStringProperty("ducc.broker.protocol");
        String host  = ducc_properties.getStringProperty("ducc.broker.hostname");
        String port  = ducc_properties.getStringProperty("ducc.broker.port");
        return proto + "://" + host + ":" + port;
    }

    
    // replicates ServiceDescription toString in order to test the interfaces
    String serviceToString(IServiceDescription desc)
    {
        StringBuffer sb = new StringBuffer();

        String type = desc.getType().toString();

        sb.append("XService: ");
        sb.append(type);
        sb.append(":");
        sb.append(desc.getEndpoint());

        if ( type.equals("UimaAs") ) {
            sb.append(":");
            sb.append(desc.getBroker());
        }
        sb.append("\n");

        String subclass = desc.getSubclass().toString();
        sb.append("   Service Class     : ");
        sb.append(subclass);
        if ( subclass.equals("Registered")) {
            sb.append(" as ID ");
            sb.append(desc.getId());
            sb.append(" Owner[");
            sb.append(desc.getUser());
            sb.append("] instances[");
            sb.append(Integer.toString(desc.getInstances()));
            sb.append("] linger[");
            sb.append(Long.toString(desc.getLinger()));
            sb.append("]");
        }
        sb.append("\n");

        sb.append("   Implementors      : ");
        Long[] implementors = desc.getImplementors();
        if ( implementors.length > 0 ) {
            for (Long id : implementors) {
                sb.append(id);
                sb.append(" ");
            }
        } else {
            sb.append("(N/A)");
        }
        sb.append("\n");
         
        sb.append("   References        : ");
        Long[] references = desc.getReferences();
        if ( references.length > 0 ) {
            for ( Long id : references ) {
                sb.append(id);
                sb.append(" ");
            }
        } else {
            sb.append("None");
        }
        sb.append("\n");
       
        sb.append("   Dependencies      : ");
        Map<String, String> dependencies = desc.getDependencies();
        if ( dependencies == null ) {
            sb.append("none\n");
        } else {
            sb.append("\n");
            for ( String s : dependencies.keySet() ) {
                sb.append("      ");
                sb.append(s);
                sb.append(": ");
                sb.append(dependencies.get(s));
                sb.append("\n");
            }
        }

        sb.append("   Service State     : ");
        sb.append(desc.getServiceState());
        sb.append("\n");

        sb.append("   Ping Active       : ");
        sb.append(desc.isActive());
        sb.append("\n");

        sb.append("   Start Mode        : ");
        boolean autostart = desc.isAutostart();
        boolean reference_start = desc.isReferenceStart();

        if ( autostart )            { sb.append("autostart"); }
        else if ( reference_start ) { sb.append("reference"); }
        else {
            if ( implementors.length > 0 ) { 
                sb.append("manual"); 
            } else {
                sb.append("stopped");
            }
        }
        
        if ( desc.isEnabled() ) {
            sb.append(", Enabled");
        } else {
            sb.append(", Disabled; reason: ");
            sb.append(desc.getDisableReason());
        }
        sb.append("\n");

        sb.append("   Last Use          : ");
        sb.append(desc.getLastUseString());
        sb.append("\n");

        sb.append("   Registration Date : ");
        sb.append(desc.getRegistrationDate());
        sb.append("\n");

        String error_string = desc.getErrorString();
        if ( error_string != null ) {
            sb.append("   Errors            : ");
            sb.append(error_string);
            sb.append("\n");
        }

        sb.append("   Service Statistics: ");
        IServiceStatistics qstats = desc.getQstats();
        if ( qstats == null ) {
            sb.append("None\n");
        } else {
            sb.append("\n       ");            
            sb.append(qstats.toString());
            sb.append("\n");
        }
        return sb.toString();
    }

    List<IServiceDescription> query()
    {
        DuccServiceApi api;
        IServiceReply reply;

        System.out.println("Query");
        List<IServiceDescription> ret = null;
        try {
            api = new DuccServiceApi(null);
            reply = api.query(new String[] {"--query"} );
	   
            ret = reply.getServiceDescriptions();

			if ( ret.size() == 0 ) {
			    System.out.println("No services in query.");
			} else {
			    for (IServiceDescription d : ret) {                
			        System.out.println(serviceToString(d));         // this exercises the IServiceDescription methods
			        System.out.println("       ------------------------------");
			        System.out.println(d.toString());               // this is the actual query as returned by SM
			    }
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
        return ret;
    }

    IServiceDescription getServiceDescription(String id)
    {
        try {
			DuccServiceApi api = new DuccServiceApi(null);
			IServiceReply qreply;
			List<IServiceDescription> service_list;
			
			qreply = api.query(new String[] {"--query", id});      // general service reply
			service_list = qreply.getServiceDescriptions();        // list of service descriptions

            if ( service_list.size() == 0 ) {
                System.out.println("Query returns no results for " + id + " returning null.");
                return null;
            }

			return service_list.get(0);                            // failure here is test failure, no need to check too carefully
		} catch (Exception e) {
            System.out.println("Query failed with exception, returning null.");
			e.printStackTrace();
            return null;
		}
    }

    boolean waitForState(String desired_state, String id)
    {
        return waitForState(desired_state, id, default_timeout);
    }

    boolean waitForState(String desired_state, String id, int timeout)
    {
        try {
            int count = 0;
            String prev = "";
            System.out.println("Waiting for service " + id + " to reach state " + desired_state + " timeout " + timeout);
            do {
                IServiceDescription desc = getServiceDescription(id);
                if ( desc == null ) {
                    return false;
                }

                String state = desc.getServiceState().toString();
                if ( ! state.equals(prev) ) count = 0;

                if ( state.equals(desired_state) ) return true;

                if ( ++count > timeout ) {
                    System.out.println("Query times out after " + count + " tries.");
                    return false;
                }
                prev = state;
			    System.out.println(" ... " + state);
			    try { Thread.sleep(5000); } catch (InterruptedException e ) {}
			} while ( true );
//		} catch (InterruptedException e) {
//            // I see this if the MR fails early - it interrupts the waiter which is me.
//            System.out.println("Query failed with sleep interruption.");            
//            return false;
		} catch (Exception e) {
            System.out.println("Query failed with exception:");            
			e.printStackTrace();
			return false;
		}
    }


    boolean waitForStartState(String desired_state, String id)
    {
        try {
            int count = 0;
            System.out.println("Waiting for service " + id + " to reach start state " + desired_state);

            do {
                IServiceDescription desc = getServiceDescription(id);
                Long[] implementors = desc.getImplementors();

                if ( desc.isAutostart()      && desired_state.equals("autostart") ) return true;
                if ( desc.isReferenceStart() && desired_state.equals("reference") ) return true;

                if ( !desc.isAutostart() && (!desc.isReferenceStart()) ) {
                    if ( implementors.length >  0 && desired_state.equals("manual") ) return true;
                    if ( implementors.length == 0 && desired_state.equals("stopped") ) return true;
                }
                
                if ( ++count > default_timeout ) {
                    System.out.println("Query times out after " + count + " tries.");
                    return false;                    
                }
                System.out.println(" ... autostart " + desc.isAutostart() + " reference " + desc.isReferenceStart() + " n_implementors " + implementors.length);
			    Thread.sleep(5000);
			} while ( true );
		} catch (Exception e) {
            System.out.println("Query failed with exception:");
			e.printStackTrace();
            return false;
		}
    }

    String getEndpoint(String id)
    {
        try {
            IServiceDescription desc = getServiceDescription(id);
            if ( desc == null ) {
                return null;
            }
            
            switch ( desc.getType() ) {
                case UimaAs:
                    return "UIMA-AS:" + desc.getEndpoint() + ":" + desc.getBroker();
                case Custom:
                    return "CUSTOM:" + desc.getEndpoint();
                default:
                    System.out.println("Unrecognized service type in getEndoint: " + desc.getType().toString());
                    return null;
            }                 
		} catch (Exception e) {
            System.out.println("Query failed with exception:");            
			e.printStackTrace();
			return null;
		}
    }

    boolean mrStarted = false;
    boolean mrCanceled = false;
    String mrDuccId;
    boolean startMr()
    {
        mrStarted = false;
        mrCanceled = false;

        MrRunner mr = new MrRunner(Thread.currentThread());
        Thread th = new Thread(mr);
        th.start();

        while ( ! mrStarted ) {
            try {
                Thread.sleep(2000);
            } catch ( InterruptedException e ) {
                break;
            }
        }
        return mrStarted;
    }

    class MrRunner
        implements Runnable
    {
        Thread waiter;
        MrRunner(Thread waiter)
        {
            this.waiter = waiter;
        }

        public void run()
        {
            Properties reserve_props = new Properties();
            reserve_props.setProperty("description", "Anonymous Service for Ping-only testing.");
            reserve_props.setProperty("process_memory_size", "2");
            reserve_props.setProperty("process_executable", ducc_properties.getProperty("ducc.jvm"));
            reserve_props.setProperty("process_executable_args", "-cp ${DUCC_HOME}/lib/uima-ducc/examples/* org.apache.uima.ducc.test.service.AnonymousService ${HOME}/ducc/logs/service");
            reserve_props.setProperty("scheduling_class", "fixed");
            reserve_props.setProperty("wait_for_completion", "true");

            try {
				DuccManagedReservationSubmit pingOnlyService = new DuccManagedReservationSubmit(reserve_props, new MrCallback("Running"));
				if ( pingOnlyService.execute() ) {
                    mrDuccId = ""+pingOnlyService.getDuccId();
				    System.out.println("Managed reservation " + mrDuccId + " submitted successfully, rc =" + pingOnlyService.getReturnCode());
				} else {
				    System.out.println("Managed reservation submit failed, rc = " + pingOnlyService.getReturnCode());
				}
			} catch (Exception e) {
                System.out.println("Submit anonymous service (MR) failed with exception.");
				e.printStackTrace();
                waiter.interrupt();
			}
            
            waiter.interrupt();
        }
    }

    interface ApiRunner
    {
        void setStatus(String status);
        String getStatus();
        long getId();
    }

    class JobRunner
        implements Runnable, ApiRunner
    {
        String status = "<none>";
        DuccJobSubmit submit;
        Long id = -1L;
        String service_dependency;

        boolean exited = false;

        JobRunner(String service_dependency)
        {
            this.service_dependency = service_dependency;
        }

        public void run()
        {
            // start a job that depends on this service
            Properties props = new Properties();

            props.setProperty("driver_descriptor_CR",           "org.apache.uima.ducc.test.randomsleep.FixedSleepCR");
            props.setProperty("driver_descriptor_CR_overrides", "jobfile=${DUCC_HOME}/examples/simple/1.inputs compression=10 error_rate=0.0");
            props.setProperty("driver_jvm_args",                "-Xmx500M");            
            props.setProperty("process_descriptor_AE",          "org.apache.uima.ducc.test.randomsleep.FixedSleepAE");
            props.setProperty("process_memory_size",            "2");

            String dh = "${DUCC_HOME}";            
            String classpath = dh + "/lib/uima-ducc/examples/*";
            classpath = classpath + ":/" + dh + "/apache-uima/lib/*";
            classpath = classpath + ":/" + dh + "/apache-uima/apache-activemq/lib/*";
            classpath = classpath + ":/" + dh + "/apache-uima/apache-activemq/lib/optional/*";

            props.setProperty("classpath",                      classpath);
            props.setProperty("process_jvm_args",               "-Xmx100M ");
            props.setProperty("process_thread_count",           "2");
            props.setProperty("process_per_item_time_max",      "5");
            props.setProperty("environment",                    "AE_INIT_TIME=1 AE_INIT_RANGE=1 INIT_ERROR=0");
            props.setProperty("process_deployments_max",        "999");
            props.setProperty("scheduling_class",               "normal");
            props.setProperty("service_dependency",             service_dependency);
            props.setProperty("description",                    "Depdendent job for Services Test");
            props.setProperty("wait_for_completion",            "true");
            props.setProperty("timestamp",                      "true");

            try {
                submit = new DuccJobSubmit(props, new ApiCallback(this));
                
                if ( submit.execute() ) {
                    System.out.println("Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
                    this.id = submit.getDuccId();
                } else {
                    System.out.println("Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
                    setStatus("Failed");
                }

			} catch (Exception e) {
                System.out.println("Submit anonymous service (MR) failed with exception.");
				e.printStackTrace();
                setStatus("Failed");
			}            
            exited = true;
        }

        public long getId() { return submit.getDuccId(); }
        
        String[] lastStatus = new String[] {"Completed", "Failed"};
        synchronized public void setStatus(String status)
        {
            System.out.println("S-->" + status);
            for (String s : lastStatus ) {
                if ( ! this.status.contains(s) ) {           // stop recording once we have nothing useful left
                    this.status = status;
                }
            }
        }

        synchronized public String getStatus()
        {
            if ( exited ) {
                return "Completed";
            } else {
                return status;
            }
        }
    }

    boolean waitForJobState(ApiRunner runner, String[] states, int timeout)
    {
        int sleeptime = 1000;
        int iterations = timeout / sleeptime;
        int count = 0;
        
        String prev_status = "";
        while ( true ) {                                        
            String status = runner.getStatus();
            if ( !prev_status.equals(status) ) count = 0;   // reset count on each state change

            // System.out.println("======== wait for completion of " + Arrays.toString(states) + ". Current status:" + status);
            if ( runner.getStatus() == "Failed" ) return false;
            for ( String s : states ) {
                if ( status.contains(s) ) return true;
            }

            if ( (timeout > 0) && (++count > iterations) ) {
                System.out.println("Timeout waiting for state " + Arrays.toString(states) + " for job " + runner.getId());
                return false;
            }
            prev_status = status;
            
            try {
                Thread.sleep(sleeptime);
            } catch (InterruptedException e) {
                System.out.println("Interruption waiting for states " + Arrays.toString(states) + " for job " + runner.getId());
                return false;
            } catch (Exception e) {
                System.out.println("Fatal error waiting for state " + Arrays.toString(states) + " for job " + runner.getId());
                e.printStackTrace();
                return false;
            }
        }
    }

    ApiRunner startJob(String dependency)
    {        
        JobRunner runner = new JobRunner(dependency);
        Thread th = new Thread(runner);
        th.start();
        return runner;
    }

    boolean startService(String id)
    	throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.start(new String[] {"--start", "" + id });
        try {
            if ( reply.getReturnCode() ) {
                System.out.println("Service " + reply.getId() + " start requested " + reply.getEndpoint());
                return true;
            } else {
                System.out.println("Service start failied: " + reply.getMessage());
                return false;
            }
        } catch ( Exception e ) {
            System.out.println("Service " + id + " start failed due to exception.");
            e.printStackTrace();
            return false;
        }
    }

    boolean stopService(String service_id)
    {
        DuccServiceApi api = new DuccServiceApi(null);
        try {
			IServiceReply reply = api.stop(new String[] {"--stop", "" + service_id});
			if ( reply.getReturnCode() ) {
			    System.out.println("Service " + reply.getId() + " stop requested " + reply.getEndpoint());
			} else {
			    System.out.println("Service stop failied: " + reply.getMessage());
			    return false;
			}
		} catch (Exception e) {
            System.out.println("Service " + service_id + " stop failed due to exception.");
			e.printStackTrace();
            return false;
		}
        System.out.println("Service is stopped");
        return true;
    }

    boolean modifyService(String service_id, String k, String v)
    {
        DuccServiceApi api = new DuccServiceApi(null);
        try {
			IServiceReply reply = api.modify(new String[] {"--modify", "" + service_id, k, v}); // again, so we can continue
			if ( reply.getReturnCode() ) {
			    System.out.println("Service modified: " + reply.getMessage() + ": " + k + " = " + v);
			    return true;
            } else {
			    System.out.println("Service " + service_id + " modify failed :" + k + " = " + v);
			    return false;
			} 
		} catch (Exception e) {
			System.out.println("Modify failed due to exception.");
			e.printStackTrace();
		}
        return false;
    }

    boolean enableService(String service_id)
    {
        DuccServiceApi api = new DuccServiceApi(null);
        try {
			IServiceReply reply = api.enable(new String[] {"--enable", "" + service_id});  // so disable has an effect
			if ( reply.getReturnCode() ) {
			    System.out.println("Service " + reply.getId() + " enable requested " + reply.getEndpoint());
                return true;
			} else {
			    System.out.println("Service enable failied: " + reply.getMessage());
			}
		} catch (Exception e) {
			System.out.println("Enable failed due to exception.");
			e.printStackTrace();
		}
        return false;
    }

    boolean ignoreReferences(String service_id)
    {

        DuccServiceApi api = new DuccServiceApi(null);
        try {
            IServiceReply reply = api.ignoreReferences(new String[] {"--ignore_references", "" + service_id}); // again, so we can continue
            if ( reply.getReturnCode() ) {
                System.out.println("Service " + reply.getId() + " ignore references issued to " + reply.getEndpoint());
                return true;
            } else {
                System.out.println("Service ignore references fails in API " + reply.getMessage());
            }
        } catch ( Exception e ) {
			System.out.println("Ignore references failed due to exception.");
			e.printStackTrace();            
        }

        return false;
    }

    boolean observeReferences(String service_id)
    {

        DuccServiceApi api = new DuccServiceApi(null);
        try {
            IServiceReply reply = api.observeReferences(new String[] {"--observe_references", "" + service_id});
            if ( reply.getReturnCode() ) {
                System.out.println("Service " + reply.getId() + " observe references issued to " + reply.getEndpoint());
                return true;
            } else {
                System.out.println("Service observe references fails in API " + reply.getMessage());
            }
        } catch ( Exception e ) {
			System.out.println("Observe references failed due to exception.");
			e.printStackTrace();            
        }

        return false;
    }

    String registerService(String[] args)
    	throws Exception
    {
        DuccServiceApi api = new DuccServiceApi(null);
        IServiceReply reply = api.register(args);
        String service_id = "<none>";
        
        if ( reply.getReturnCode() ) {
            service_id = Long.toString(reply.getId());
            System.out.println("Service " + reply.getId() + " registered " + reply.getEndpoint());
            return service_id;
        } else {
            System.out.println("Service register failied: " + reply.getMessage());
            return null;
        }
    }

    boolean unregisterService(String id)
    {
        System.out.println("Unregistering " + id);
        try {
			DuccServiceApi api = new DuccServiceApi(null);
			IServiceReply reply = api.unregister(new String[] {"--unregister", "" + id} );
			if ( reply.getReturnCode() ) {
			    System.out.println("Service " + reply.getId() + " unregistered: " + reply.getEndpoint());
			    return true;
			} else {
			    System.out.println("Service unregister failied: " + reply.getMessage());
                return false;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}   
        return false;
    }             

    void testRegister(String subtestId)
    	throws Exception
    {
        service_id = registerService(service_props);
        if ( service_id == null ) {
            fail(testId, subtestId, "Service test failed: can't register service.");
        }
        success(testId, subtestId, "Service is registered for manual start.");
        endpoint = getEndpoint(service_id);       // should be comon throughout the tests
    }

    void testManualStart(String subtestId)
    	throws Exception
    {
        if ( startService(service_id) ) {      // first start after registration
            success(testId, subtestId, "Manual service start issued.");
        } else {
            fail(testId, subtestId, "Manual service start failed.");
        }
        if ( waitForState("Available", service_id, 120000) ) {
            success(testId, subtestId, "Service is manually started.");
        } else {
            fail(testId, subtestId, "State Available timeout, state did not occur manually starting pinger for service " + service_id);
        }
    }

    void testManualStop(String subtestId, String reason)
    	throws Exception
    {
        if ( stopService(service_id) ) {
            success(testId, subtestId, "Manual service stop issued - " + reason);
        } else {
            fail(testId, subtestId, "Manual service stop failed - " + reason);
        }
        if ( waitForState("Stopped", service_id) ) {
            success(testId, subtestId, "Service is manually stopped - " + reason);
        } else {
            fail(testId, subtestId, "Service did not reach Stopped state - " + reason);
        }
    }

    void testManualRestart(String subtestId)
        throws Exception
    {
        if ( startService(service_id))  {                //r restart, verifies the previous stop cleaned up ok in SM
            success(testId, subtestId, "Second Manual service start issued.");
        } else {
            fail(testId, subtestId, "Second Manual service start failed.");
        }
        if ( waitForState("Available", service_id) ) {
            success(testId, subtestId, "Service is manually restarted");
        } else {
            fail(testId, subtestId, "State Stopped timeout, state did not occur manually restarting pinger.");
        }
    }       

    void testModifyAutostartTrue(String subtestId)
        throws Exception
    {
        if ( modifyService(service_id, "--autostart", "true")) {   // switch to autostart
            success(testId, subtestId, "Modify --autostart=true issued.");
        } else {
            fail(testId, subtestId, "Modify --autostart=true failed.");
        }  
        if ( waitForStartState("autostart", service_id) ) {
            success(testId, subtestId, "Service is modified for autostart.");
        } else {
            fail(testId, subtestId, "Service modify for --autostart failed.");
        }
    }

    void testEnableFromAutostart(String subtestId)
        throws Exception
    {
        if ( enableService(service_id) )  {       // let it enable out of autostart
            success(testId, subtestId, "Enable issued to stopped/autostart service.");
        } else {
            fail(testId, subtestId, "Enable to stopped/autostart service failed.");
        }  
        if ( waitForState("Available", service_id) ) {
            success(testId, subtestId, "Service is enabled and running with autostart enabled.");
        } else {
            fail(testId, subtestId, "Service failed to start after autostart is enacled.");
        }
    }

    void testModifyAutostartFalse(String subtestId)
        throws Exception
    {
        if ( modifyService(service_id, "--autostart", "false")) { 
            success(testId, subtestId, "Modify --autostart=false issued.");
        } else {
            fail(testId, subtestId, "Modify --autostart=true failed.");
        }  
        if ( waitForStartState("manual", service_id) ) {
            success(testId, subtestId, "Service is in manual start mode");
        } else {
            fail(testId, subtestId, "Service modify for --autostart=false failed.");
        
        }
    }

    void testEnableFromManual(String subtestId)
        throws Exception
    {
        if ( enableService(service_id) )  {       // let it enable out of autostart
            success(testId, subtestId, "Enable issued to stopped service, prep for reference tests");
        } else {
            fail(testId, subtestId, "Enable to stopped service, prep for reference tests.");
        }  
        if ( waitForStartState("stopped", service_id) ) {
            success(testId, subtestId, "Service is ready for reference tests..");
        } else {
            fail(testId, subtestId, "Service did not reach stopped mode in prep for reference tests.");
        }
    }

    void testReferenceStart_1(String subtestId)
        throws Exception
    {
        // not a test perse, but convenient to run as one
        System.out.println(testId + "          Reference <--> Manual start mode                                             -------"); 
        System.out.println(testId + "          Start with reference start, then ignore references, service should not exit. -------");
        System.out.println(testId + "          Then observe references, service should exit.                                -------");

        job = startJob(endpoint);
        if ( waitForJobState(job, new String[] {"Failed", "WaitingForServices", "WaitingForResources", "Assigned", "Initializing", "Running"}, 60000) ) {
            success(testId, subtestId, "Dependent job " + job.getId() + " submitted.");
        } else {
            fail(testId, subtestId, "Could not start dependent job.");
        }

        System.out.println(subtestId + ": Waiting for service to switch to reference start.");
        if ( waitForStartState("reference", service_id) ) {
            success(testId, subtestId, "Service is now reference started.");
        } else {
            fail(testId, subtestId, "Service did not become reference started.");
        }

        if ( waitForState("Available", service_id, default_timeout) ) {
            success(testId, subtestId, "Service is started by job. Waiting for job to complete.");
        } else {
            fail(testId, subtestId, "Service did not reference start.");
        }

        // Wait for job to actually start
        if ( waitForJobState(job, new String[] {"Running"}, 60000) ) {
            success(testId, subtestId, "Job " + job.getId() + " is started.");
        } else {
            fail(testId, subtestId, "Job " + job.getId() + " did not start within a reasonable time.");
        }

        // Now ignore refs and make sure SM agrees
        if ( ignoreReferences(endpoint) ) {
            success(testId, subtestId, "Ignore references.");
        } else {
            fail(testId, subtestId, "Ignore references");
        }
        if ( waitForStartState("manual", service_id) ) {
            success(testId, subtestId, "Service switched to manual.");
        } else {
            fail(testId, subtestId, "Service could not switch to manual..");
        }

        // Now wait for job to go away
        waitForJobState(job, new String[] {"Completed", "Failed", "completion"}, -1);
        System.out.println("Job " + job.getId() + " has exited.");

        // Now wait for linger stop which should not happen
        System.out.println("--- Waiting for service to linger stop, which we expect to fail as we should be in manual state ---");
        if ( !waitForState("Stopped", service_id, default_timeout ) ) {
            success(testId, subtestId, "Service correctly did not linger-stop");
        } else {
            fail(testId, subtestId, "Service incorrectly linger stopped, should have been manual");
        }

        if ( observeReferences(endpoint) ) {           // turn on referenced mode and expect a quick lingering stop
            success(testId, subtestId, "Observe references.");
        } else {
            fail(testId, subtestId, "Observe references");
        }
        if ( waitForStartState("reference", service_id) ) {
            success(testId, subtestId, "Service switched to reference.");
        } else {
            fail(testId, subtestId, "Service could not switch to reference..");
        }

        if ( waitForState("Stopped", service_id, default_timeout) ) {
            success(testId, subtestId, "Linger-stop service exited.");
        } else {
            fail(testId, subtestId, "Linger-stop service did not exit.");
        }
    }

    void testReferenceStart_2(String subtestId)
    	throws Exception
    {

        System.out.println(testId + "          Autostart -> reference                                                       -------"); 
        System.out.println(testId + "          Set service to autostart, then submit job.  When the job starts switch to    -------");
        System.out.println(testId + "          reference start.  When the job exits the service should also exit within its -------");
        System.out.println(testId + "          linger time.                                                                 -------");

        // the previous test should leave the service linger-stopped so simply changing the registration shoul dstart it
        System.out.println(subtestId + ": Switching service to autostart.");
        if ( modifyService(service_id, "--autostart", "true")) {   // switch to autostart
            success(testId, subtestId, "Modify --autostart=true issued.");
        } else {
            fail(testId, subtestId, "Modify --autostart=true failed.");
        }  
        // Wait for the service to start, proving the registration change worked
        System.out.println(subtestId + ": Waiting for service to become fully available.");
        if ( waitForState("Available", service_id, default_timeout) ) {
            success(testId, subtestId, "Service is autostarted.");
        } else {
            fail(testId, subtestId, "Service did not autostart.");
        }

        // get the job into running state
        System.out.println(subtestId + ": Starting a job.");
        job = startJob(endpoint);
        if ( waitForJobState(job, new String[] {"Running"}, 60000) ) {
            success(testId, subtestId, "Dependent job " + job.getId() + " submitted.");
        } else {
            fail(testId, subtestId, "Could not start dependent job.");
        }

        // turn off autostart
        System.out.println(subtestId + ": Turning off autostart.");
        if ( modifyService(service_id, "--autostart", "false")) {   // switch off autostart
            success(testId, subtestId, "Modify --autostart=false issued.");
        } else {
            fail(testId, subtestId, "Modify --autostart=false failed.");
        }  

        System.out.println(subtestId + ": Waiting for service to switch to manual start.");
        if ( waitForStartState("manual", service_id) ) {
            success(testId, subtestId, "Service is now in manual mode.");
        } else {
            fail(testId, subtestId, "Service did not switch to manual.");
        }

        System.out.println(subtestId + ": Switching to reference mode.");
        if ( observeReferences(endpoint) ) {           // turn on referenced mode and expect a quick lingering stop
            success(testId, subtestId, "Observe references.");
        } else {
            fail(testId, subtestId, "Observe references");
        }
        if ( waitForStartState("reference", service_id) ) {
            success(testId, subtestId, "Service switched to reference.");
        } else {
            fail(testId, subtestId, "Service could not switch to reference..");
        }

        
        System.out.println(subtestId + ": Waiting for job to complete.");
        if ( waitForJobState(job, new String[] {"Completed", "Failed"}, 60000) ) {
            success(testId, subtestId, "Dependent job " + job.getId() + " completed.");
        } else {
            fail(testId, subtestId, "Dependend job did not complete as expected.");
        }

        // Finally, make sure the service stops
        System.out.println(subtestId + ": Waiting for service to linger-stop.");
        if ( waitForState("Stopped", service_id, default_timeout) ) {
            success(testId, subtestId, "Service is linger-stopped.");
        } else {
            fail(testId, subtestId, "Service did not linger-stop");
        }

    }

    void testReferenceStart_3(String subtestId)
        throws Exception
    {
        subtestId = "Reference test 3";
        System.out.println(testId + "          Reference -> Autostart                                                       -------"); 
        System.out.println(testId + "          Let service reference start, then set to autostart.  Service should not exit -------");
        System.out.println(testId + "          once the job completes.                                                      -------");

        // get the job into running state
        System.out.println(subtestId + ": Starting a job.");
        job = startJob(endpoint);

        System.out.println("Waiting for service to acknowledge reference start.");
        if ( waitForStartState("reference", service_id) ) {
            success(testId, subtestId, "Service switched to reference.");
        } else {
            fail(testId, subtestId, "Service could not switch to reference..");
        }

        System.out.println(subtestId + ": Waiting for service to become fully available.");
        if ( waitForState("Available", service_id, default_timeout) ) {
            success(testId, subtestId, "Service is reference started.");
        } else {
            fail(testId, subtestId, "Service did not start correctly.");
        }

        System.out.println(subtestId + ": Waiting for job to start.");
        if ( waitForJobState(job, new String[] {"Running"}, 60000) ) {
            success(testId, subtestId, "Dependent job " + job.getId() + " submitted.");
        } else {
            fail(testId, subtestId, "Could not start dependent job.");
        }

        System.out.println(subtestId + ": Switching service to autostart.");
        if ( modifyService(service_id, "--autostart", "true")) {   // switch to autostart
            success(testId, subtestId, "Modify --autostart=true issued.");
        } else {
            fail(testId, subtestId, "Modify --autostart=true failed.");
        }  

        System.out.println(subtestId + ": Wait for autostart modify to complete..");
        if ( waitForStartState("autostart", service_id) ) {
            success(testId, subtestId, "Service switched to autostart.");
        } else {
            fail(testId, subtestId, "Service could not switch to autostart..");
        }

        System.out.println(subtestId + ": Waiting for job to complete.");
        if ( waitForJobState(job, new String[] {"Completed", "Failed"}, 60000) ) {
            success(testId, subtestId, "Dependent job " + job.getId() + " completed.");
        } else {
            fail(testId, subtestId, "Dependend job did not complete as expected.");
        }

        // Finally, make sure the service does NOT stop
        System.out.println(subtestId + ": Insuring the service does not stap.");
        if ( waitForState("Stopped", service_id, default_timeout) ) {
            fail(testId, subtestId, "Service stopped when it should not have.");
        } else {
            success(testId, subtestId, "Service correctly did not stop");
        }
    }


    void testUnregister(String subtestId)
    	throws Exception
    {
        if ( unregisterService(service_id) ) {  // finish the test
            success(testId, "<complete>", "Unregister issued.");
        } else {
            fail(testId, "<complete>", "Unregister failed.");
        }    
    }        

    void runTestSet(String[] testset)
    	throws Exception
    {
        for ( int i = 0; i < testset.length; i++ ) {
            String sti = testset[i];


            System.out.println(testId + " -------------- START -------------------- " + sti + " ---------- START ------------");
            if ( sti.equals("ManualStop") ) {
                // stop happens for a lot of reasons, so we include a reason string
                Method m = getClass().getDeclaredMethod("test" + sti, String.class, String.class);
                String reason = testset[++i];
                m.invoke(this, new Object[] {sti, reason} );
            } else {
                try {
                    Method m = getClass().getDeclaredMethod("test" + sti, String.class);
                    m.invoke(this, new Object[] {sti,} );
                } catch ( Exception e ) {
                	Throwable cause = e.getCause();
                	System.out.println(cause.toString());
                	cause.printStackTrace();
                }
            }
            System.out.println(testId + " -------------- END ---------------------- " + sti + " ---------- END --------------");
            System.out.println(" ");
        }
    }


    void runTests(String testId, String[] service_props)
    	throws Exception
    {
        //x - register an MR as some anonymous remore service
        //x - register a ping-only guy to ping him
        //x -   wait for Available
        //x -   stop the pinger
        // -   start the pinger and wait for available
        // -   stop the pinger
        // -   modify the registration to autorun
        // -   enable the pinger and wait for available
        // -   stop the pinger and wait for stopped
        // -   enable the pinger and wait for available
        // -   cancel the MR service
        // -   after stopping the service we want to see the pinger go to waiting
        // -   unregister the pinger
        // - stop the MR

        //
        // We run these complext tests in groups to make debug easier
        //

        //
        // Generic register, to initialize things
        String[] testset0 = {
            "Register"   ,
        };

        //
        // Basic function
        //
        String[] testset1 = {
            "ManualStart",
            "ManualStop",   "Basic test",           // all ManualStop must include a reason string as well
            "ManualRestart",
            "ManualStop",   "Prep for autostart tests",
            "ModifyAutostartTrue", 
            "EnableFromAutostart",
            "ManualStop",   "Stop autostarted service.",
            "EnableFromAutostart",                  // we do this again to make sure state didn't get messed up so far
            "ModifyAutostartFalse",                 // go into manual
            "ManualStop",   "Prep for reference tests", // stopt the service
            "EnableFromManual",                     // make it startable
        };

        //
        // Reference start, manual <--> reference
        //
        String[] testset2 = {
            "ReferenceStart_1",
        };

        //
        // Reference start, autostart ---> reference
        //
        String[] testset3 = {
            "ReferenceStart_2",
        };

        //
        // Reference start, reference ---> autostart
        //
        String[] testset4 = {
            "ReferenceStart_3",
        };

        //
        // Generic unregister to finish up
        //
        String[] testset99 = {
            "Unregister",
        };

        this.testId = testId;
        this.service_props = service_props;
        runTestSet(testset0);
        runTestSet(testset1);
        runTestSet(testset2);
        runTestSet(testset3);
        runTestSet(testset4);
        runTestSet(testset99);
                
    }

    void runPingOnly()
    	throws Exception
    {
    	String testId = "Ping-Only";

        if ( startMr() ) {         // hangs until it's running
            success(testId, "<init MR>", "Ping-only service test: anonymous service started as MR.");
        } else {
            fail(testId, "<init MR>", "Cannot start anonymous service; ping-only test failed.");
        }
        
        String[] service_props = {
            "--description"              , "Custom Pinger",
            "--service_request_endpoint" , "CUSTOM:AnonymousPinger",
            "--service_ping_dolog"       , "true",
            "--service_ping_class"       , "org.apache.uima.ducc.test.service.AnonymousPinger",
            "--service_ping_classpath"   , ducc_home + "/lib/uima-ducc/examples/*",
            "--service_ping_timeout"     , "10000",
            "--service_ping_arguments"   , System.getenv("HOME") + "/ducc/logs/service/hostport",
            "--service_linger",         "10000",
        };

        try {
            runTests(testId, service_props);
        } finally {        
            if ( mrDuccId != null ) {
                mrCanceled = true;
                DuccManagedReservationCancel pingOnlyCancel = new DuccManagedReservationCancel(new String[] {"--id", mrDuccId});
                if ( pingOnlyCancel.execute() ) {
                    System.out.println("Anonymous MR service canceled");
                } else {
                    System.out.println("Anonymous MR service won't cancel, rc = " + pingOnlyCancel.getReturnCode());
                }
            }
        }

    }

    void runNormal()
    	throws Exception
    {
    	String testId = "UIMA Service";

        String dh = "${DUCC_HOME}";            
        String classpath = dh + "/lib/uima-ducc/examples/*";
        classpath = classpath + ":/" + dh + "/apache-uima/lib/*";
        classpath = classpath + ":/" + dh + "/apache-uima/apache-activemq/lib/*";
        classpath = classpath + ":/" + dh + "/apache-uima/apache-activemq/lib/optional/*";

        String[] service_props = {
            "--description",            "Test Service 1",
            "--process_jvm_args",       "-Xmx100M -DdefaultBrokerURL=",  // note broken, gets fixed in a while
            "--classpath",              classpath,
            "--service_ping_arguments", "broker-jmx-port=1099",
            "--environment",            "AE_INIT_TIME=5000 AE_INIT_RANGE=1000 INIT_ERROR=0 LD_LIBRARY_PATH=/yet/a/nother/dumb/path",
            "--process_memory_size",    "15",
            "--process_descriptor_DD",             "${DUCC_HOME}/examples/simple/resources/service/Service_FixedSleep_1.xml",
            "--scheduling_class",       "fixed",
            "--working_directory",       "${HOME}",
            "--register",		// Has no specification argument
            "--service_linger",         "10000",
        };

        service_props[3] = "-Xmx100M -DdefaultBrokerURL=" + mkBrokerString(ducc_home);  // any failure, test setup is broken, don't bother with checking
        runTests(testId, service_props);
    }

    public void doit(String[] args)
    	throws Exception
    {
        DuccServiceApi api;
        IDuccCallback cb = new MyCallback();
        IServiceReply reply;

        // Need broker location
        ducc_home = System.getProperty("DUCC_HOME");
        if ( ducc_home == null ) {
            throw new IllegalArgumentException("DUCC_HOME must be set into system properties.");
        }
        ducc_properties = new DuccProperties();
        ducc_properties.load(new FileInputStream(ducc_home + "/resources/ducc.properties"));


        // start by clearing all registrations owned by me
        System.out.println("---------------------------------------- Init and Clear ----------------------------------------");
        List<IServiceDescription> services = query();
        if ( services.size() == 0 ) {
            System.out.println("Init test: no services to clean");
        } else {
            for (IServiceDescription d : services ) {
                api = new DuccServiceApi(cb);
                System.out.println("Unregistering " + d.getId());
                reply = api.unregister(new String[] {"--unregister", "" + d.getId()});
                if ( reply.getReturnCode() ) {
                    System.out.println("Service " + reply.getId() + " unregistered: " + reply.getEndpoint());
                } else {
                    System.out.println("Service unregister failied: " + reply.getMessage());
                }                
                    
            }
        }

        //
        // Do ping-only tests
        //
//        try {
//            runPingOnly();
//        } catch ( Exception e ) {
//            System.out.println("Ping-only test failed: " + e.toString());
//        }

        //
        // Do "normal" tests
        //
        try {
            runNormal();
        } catch ( Exception e ) {
            System.out.println("Normal test failed:" + e.toString());
        }
        
            			
    }

    public static void main(String[] args)
    {
        try {
            ServiceTester st = new ServiceTester();
            st.doit(args);

            System.out.println("Test summary: successes[" + st.successReasons.size() + "] " + "failures[" + st.failReasons.size() + "]");
            System.out.println("Tests passed:");
            if ( st.successReasons.size() > 0 ) {
                for (String s : st.successReasons) {
                    System.out.println("   " + s);
                }
            } else {
                System.out.println("    None.");
            }

            System.out.println("Tests failed:");
            if ( st.failReasons.size() > 0 ) {
                for (String s : st.failReasons) {
                    System.out.println("   " + s);
                }
            } else {
                System.out.println("    None.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class MyCallback
        implements IDuccCallback
    {
        public void console(int pnum, String  msg)
        {
            System.out.println("C--> " + pnum + " " + msg);
        }

        public void status(String msg)
        {
            System.out.println("S--> " +msg);
        }
    }

    class ApiCallback
        implements IDuccCallback
    {
        ApiRunner runner;
        ApiCallback(ApiRunner runner)
        {
            this.runner = runner;
        }

        public void console(int pnum, String  msg)
        {
            // nothng
        }

        public void status(String msg)
        {
            runner.setStatus(msg);
        }
    }

    class MrCallback
        implements IDuccCallback
    {
        String waitfor;
        // This is a "real" callback, waiting for a specific state
        MrCallback(String waitfor)
        {
            this.waitfor = waitfor;
            System.out.println("Waiting for state " + waitfor);
        }

        public void console(int pnum, String  msg)
        {
            System.out.println("C--> " + pnum + " " + msg);
        }

        public void status(String msg)
        {
            System.out.println("S--> " +msg);

            if ( msg.contains(waitfor) ) {
                mrStarted = true;
            	System.out.println("     " + waitfor + " state achieved.");
            } else if ( msg.contains("Completed" ) ) {
                if ( !mrCanceled ) {
                    try {
                        fail("Ping-Only test", "<MR>", "Managed reservation exited prematurely.");
                    } catch ( Exception e ) {
                        // who cares 
                    }
                }
            } else {                
                if ( ! mrStarted ) {
                    System.out.println(msg + " did not seem to contain " + waitfor);
                }
            }
        }
    }

}

