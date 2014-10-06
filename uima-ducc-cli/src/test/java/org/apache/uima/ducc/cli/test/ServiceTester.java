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
import org.apache.uima.ducc.common.utils.id.ADuccId;
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
    
    // Base props.  These run a service from the examples
    String[] props = {
        "--description",            "Test Service 1",
        "--process_jvm_args",       "-Xmx100M -DdefaultBrokerURL=",  // note broken, gets fixed in a while
        "--classpath",              "${DUCC_HOME}/lib/uima-ducc/examples/*:${DUCC_HOME}/apache-uima/lib/*:${DUCC_HOME}/apache-uima/apache-activemq/lib/*:${DUCC_HOME}/examples/simple/resources/service",
        "--service_ping_arguments", "broker-jmx-port=1099",
        "--environment",            "AE_INIT_TIME=5000 AE_INIT_RANGE=1000 INIT_ERROR=0 LD_LIBRARY_PATH=/yet/a/nother/dumb/path",
        "--process_memory_size",    "15",
        "--process_DD",             "${DUCC_HOME}/examples/simple/resources/service/Service_FixedSleep_1.xml",
        "--scheduling_class",       "fixed",
        "--working_directory",       "${HOME}",
        "--service_linger",         "10000",
    };

    List<String> failReasons = new ArrayList<String>();
    List<String> successReasons = new ArrayList<String>();

    int default_timeout = 20;    // how many polls to allow before calling timeout
    ServiceTester()
    {
    }

    void fail(String testid, String subtestId, String reason)
        throws FailedTestException
    {
        failReasons.add(testid + " - " + subtestId + " - " + reason);
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
        successReasons.add(testid + " - " + subtestId + " - " + reason);
        System.out.println(testid + " - " + subtestId + " - test passed: " + reason);
    }

    /**
     * Must look into ducc.properties to find the broker.
     */
    String mkBrokerString(String ducc_home)
    	throws Exception
    {
        ducc_properties = new DuccProperties();
        ducc_properties.load(new FileInputStream(ducc_home + "/resources/ducc.properties"));
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

        sb.append("Service: ");
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
        List<ADuccId> implementors = desc.getImplementors();
        if ( implementors.size() > 0 ) {
            for (ADuccId id : implementors) {
                sb.append(id.getFriendly());
                sb.append(" ");
            }
        } else {
            sb.append("(N/A)");
        }
        sb.append("\n");
         
        sb.append("   References        : ");
        List<ADuccId> references = desc.getReferences();
        if ( references.size() > 0 ) {
            for ( ADuccId id : references ) {
                sb.append(id.getFriendly());
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
            if ( implementors.size() > 0 ) { 
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
            System.out.println("Waiting for service " + id + " to reach state " + desired_state);
            do {
                IServiceDescription desc = getServiceDescription(id);
                if ( desc == null ) {
                    return false;
                }

                String state = desc.getServiceState().toString();

                if ( state.equals(desired_state) ) return true;

                if ( ++count > timeout ) {
                    System.out.println("Query times out after " + count + " tries.");
                    return false;
                }

			    System.out.println(" ... " + state);
			    Thread.sleep(5000);
			} while ( true );
		} catch (InterruptedException e) {
            // I see this if the MR fails early - it interrupts the waiter which is me.
            System.out.println("Query failed with sleep interruption.");            
            return false;
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
                List<ADuccId> implementors = desc.getImplementors();

                if ( desc.isAutostart()      && desired_state.equals("autostart") ) return true;
                if ( desc.isReferenceStart() && desired_state.equals("reference") ) return true;

                if ( !desc.isAutostart() && (!desc.isReferenceStart()) ) {
                    if ( implementors.size() >  0 && desired_state.equals("manual") ) return true;
                    if ( implementors.size() == 0 && desired_state.equals("stopped") ) return true;
                }
                
                if ( ++count > default_timeout ) {
                    System.out.println("Query times out after " + count + " tries.");
                    return false;                    
                }
                System.out.println(" ... autostart " + desc.isAutostart() + " reference " + desc.isReferenceStart() + " n_implementors " + implementors.size());
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
            props.setProperty("classpath",                      "${DUCC_HOME}/lib/uima-ducc/examples/*");
            props.setProperty("process_jvm_args",               "-Xmx100M ");
            props.setProperty("process_thread_count",           "2");
            props.setProperty("process_per_item_time_max",      "5");
            props.setProperty("environment",                    "AE_INIT_TIME=5 AE_INIT_RANGE=5 INIT_ERROR=0 LD_LIBRARY_PATH=/yet/a/nother/dumb/path");
            props.setProperty("process_deployments_max",        "999");
            props.setProperty("scheduling_class",               "normal");
            props.setProperty("service_dependency",             service_dependency);
            props.setProperty("description",                    "Depdendent job for Services Test");
            props.setProperty("wait_for_completion",            "true");

            try {
                submit = new DuccJobSubmit(props, new ApiCallback(this));
                
                System.out.println("------------------------------ Submit Job for Services Test ------------------------------");
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
        
        while ( true ) {                                        
            String status = runner.getStatus();
            // System.out.println("======== wait for completion of " + Arrays.toString(states) + ". Current status:" + status);
            if ( runner.getStatus() == "Failed" ) return false;
            for ( String s : states ) {
                if ( status.contains(s) ) return true;
            }

            if ( (timeout > 0) && (++count > iterations) ) {
                System.out.println("Timeout waiting for state " + Arrays.toString(states) + " for job " + runner.getId());
                return false;
            }

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
        System.out.println("---------------------------------------- Stop ----------------------------------------");
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
        System.out.println("---------------------------------------- Modify ----------------------------------------");
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
        System.out.println("---------------------------------------- Enable ----------------------------------------");
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
        System.out.println("---------------------------------------- Ignore References ----------------------------------------");

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
        System.out.println("---------------------------------------- Observe References ----------------------------------------");

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

        String    subtestId;
        String    endpoint;
        ApiRunner job;


        System.out.println(testId + " ---------------------------------------- Register  ------------------------------");
        String service_id = registerService(service_props);
        if ( service_id == null ) {
            fail(testId, "<init>", "Service test failed: can't register service.");
        }
        success(testId, "<init>", "Service is registered for manual start.");
        endpoint = getEndpoint(service_id);       // should be comon throughout the tests

        subtestId = "Basic function";
        System.out.println(testId + "---------------------------------------- Manual Start ------------------------------");
        if ( startService(service_id) ) {      // first start after registration
            success(testId, subtestId, "Manual service start issued.");
        } else {
            fail(testId, subtestId, "Manual service start failed.");
        }
        if ( waitForState("Available", service_id) ) {
            success(testId, subtestId, "Service is manually started.");
        } else {
            fail(testId, subtestId, "State Available timeout, state did not occur manually starting pinger.");
        }

        System.out.println(testId + "---------------------------------------- Manual Stop ------------------------------");
        if ( stopService(service_id) ) {
            success(testId, subtestId, "Manual service stop issued.");
        } else {
            fail(testId, subtestId, "Manual service stop failed.");
        }
        if ( waitForState("Stopped", service_id) ) {
            success(testId, subtestId, "Service is manually stopped");
        } else {
            fail(testId, subtestId, "State Stopped timeout, state did not occur manually stopping pinger.");
        }
            
        System.out.println(testId + "---------------------------------------- Manual Restart ------------------------------");
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
            

        System.out.println(testId + "---------------------------------------- Manual Stop, prep for Autostart tests ------------------------------");
        if ( stopService(service_id)) {
            success(testId, subtestId, "Manual service stop issued in preparation for autostart tests.");
        } else {
            fail(testId, subtestId, "Manual service stop failed preparing for autostart tests.");
        }
        if ( waitForState("Stopped", service_id) ) {
            success(testId, subtestId, "Service is manually is stopped in preparation for autostart tests.");
        } else {
            fail(testId, subtestId, "State Stopped timeout, state did not occur manually stopping pinger in preparation for autostart tests.");
        }

        System.out.println(testId + "---------------------------------------- Modify autostart=true ------------------------------");
        if ( modifyService(service_id, "--autostart", "true")) {   // switch to autostart
            success(testId, subtestId, "Modify --autostart=true issued.");
        } else {
            fail(testId, subtestId, "Modify --autostart=true failed.");
        }  
        if ( waitForStartState("autostart", service_id) ) {
            success(testId, subtestId, "Service is modified for autostart, waiting for it to start.");
        } else {
            fail(testId, subtestId, "Service modify for --autostart failed.");
        }
            
        System.out.println(testId + "---------------------------------------- Enable and wait to become Available ------------------------------");
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

        System.out.println(testId + "---------------------------------------- Stop autostarted service ------------------------------");
        if ( stopService(service_id) )  {          // once more, make sure it will stop out of autostart
            success(testId, subtestId, "Second stop issued to autostarted service.");
        } else {
            fail(testId, subtestId, "Second stop to autostarted service failed.");
        }    
        if ( waitForState("Stopped", service_id) ) {
            success(testId, subtestId, "Service is stopped from autostart mode. Reenabling ...");
        } else {
            fail(testId, subtestId, "State Stopped timeout, did not reach stop start after stop from autostart.");
        }

        System.out.println(testId + "---------------------------------------- Re-enable autostarted service and wait for Available ------------------------------");
        if ( enableService(service_id) )  {          // once more, make sure it will restart out of autostart
            success(testId, subtestId, "Second enable issued to autostarted service.");
        } else {
            fail(testId, subtestId, "Second enable to autostarted service failed.");
        }    
        if ( waitForState("Available", service_id) ) {
            success(testId, subtestId, "Service is enabled and running after stop from autostart.");
        } else {
            fail(testId, subtestId, "Service did not restart after enable from autostart.");
        }

        System.out.println(testId + "---------------------------------------- Modify autostart=false ------------------------------");
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

        System.out.println(testId + "---------------------------------------- Manual Stop, prep for Reference tests ------------------------------");
        if ( stopService(service_id) ) {
            success(testId, subtestId, "Manual service stop issued in prep for reference tests");
        } else {
            fail(testId, subtestId, "Manual service stop failed, prep for reference tests");
        }
        if ( waitForState("Stopped", service_id) ) {
            success(testId, subtestId, "Service is manually stopped, prep for reference tests.");
        } else {
            fail(testId, subtestId, "State Stopped timeout, state did not occur manually stopping in prep for reference tests");
        }

        System.out.println(testId + "---------------------------------------- Enable in prep for reference tests ------------------------------");
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

        subtestId = "Reference test 1";
        System.out.println(testId + "---------------------------------------- "  +  subtestId  +   " ------------------------------");
        System.out.println(testId + "          Reference <--> Manual start mode                                             -------"); 
        System.out.println(testId + "          Start with reference start, then ignore references, service should not exit. -------");
        System.out.println(testId + "          Then observe references, service should exit.                                -------");
        System.out.println(testId + "---------------------------------------- Reference Start Test 1 ------------------------------");
        job = startJob(endpoint);

        if ( waitForJobState(job, new String[] {"Failed", "WaitingForResources", "Assigned", "Initializing", "Running"}, 30000) ) {
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
        
        waitForJobState(job, new String[] {"Completed", "Failed", "completion"}, -1);
        System.out.println("Job " + job.getId() + " has exited.");

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

        subtestId = "Reference test 2";
        System.out.println(testId + "---------------------------------------- "  +  subtestId  +   " ------------------------------");
        System.out.println(testId + "          Autostart -> reference                                                       -------"); 
        System.out.println(testId + "          Set service to autostart, then submit job.  When the job starts switch to    -------");
        System.out.println(testId + "          reference start.  When the job exits the service should also exit within its -------");
        System.out.println(testId + "          linger time.                                                                 -------");
        System.out.println(testId + "---------------------------------------- Reference Start Test 2 ------------------------------");

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
        if ( waitForJobState(job, new String[] {"Running"}, 30000) ) {
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
        if ( waitForJobState(job, new String[] {"Completed", "Failed"}, 30000) ) {
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


        subtestId = "Reference test 3";
        System.out.println(testId + "---------------------------------------- "  +  subtestId  +   " ------------------------------");
        System.out.println(testId + "          Reference -> Autostart                                                       -------"); 
        System.out.println(testId + "          Let service reference start, then set to autostart.  Service should not exit -------");
        System.out.println(testId + "          once the job completes.                                                      -------");
        System.out.println(testId + "---------------------------------------- Reference Start Test 2 ------------------------------");

        // get the job into running state
        System.out.println(subtestId + ": Starting a job.");
        job = startJob(endpoint);
        if ( waitForJobState(job, new String[] {"Running"}, 30000) ) {
            success(testId, subtestId, "Dependent job " + job.getId() + " submitted.");
        } else {
            fail(testId, subtestId, "Could not start dependent job.");
        }

        System.out.println("Waiting for service to acknowledge reference start.");
        if ( waitForStartState("reference", service_id) ) {
            success(testId, subtestId, "Service switched to reference.");
        } else {
            fail(testId, subtestId, "Service could not switch to reference..");
        }

        System.out.println(subtestId + ": Waiting for service to become fully available.");
        if ( waitForState("Available", service_id, default_timeout) ) {
            success(testId, subtestId, "Service is autostarted.");
        } else {
            fail(testId, subtestId, "Service did not autostart.");
        }

        System.out.println(subtestId + ": Switching service to autostart.");
        if ( modifyService(service_id, "--autostart", "true")) {   // switch to autostart
            success(testId, subtestId, "Modify --autostart=true issued.");
        } else {
            fail(testId, subtestId, "Modify --autostart=true failed.");
        }  

        System.out.println(subtestId + ": Waiting for job to complete.");
        if ( waitForJobState(job, new String[] {"Completed", "Failed"}, 30000) ) {
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


        if ( unregisterService(service_id) ) {  // finish the test
            success(testId, "<complete>", "Unregister issued.");
        } else {
            fail(testId, "<complete>", "Unregister failed.");
        }    
                
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

        String[] service_props = {
            "--description",            "Test Service 1",
            "--process_jvm_args",       "-Xmx100M -DdefaultBrokerURL=",  // note broken, gets fixed in a while
            "--classpath",              "${DUCC_HOME}/lib/uima-ducc/examples/*:${DUCC_HOME}/apache-uima/lib/*:${DUCC_HOME}/apache-uima/apache-activemq/lib/*:${DUCC_HOME}/examples/simple/resources/service",
            "--service_ping_arguments", "broker-jmx-port=1099",
            "--environment",            "AE_INIT_TIME=5000 AE_INIT_RANGE=1000 INIT_ERROR=0 LD_LIBRARY_PATH=/yet/a/nother/dumb/path",
            "--process_memory_size",    "15",
            "--process_DD",             "${DUCC_HOME}/examples/simple/resources/service/Service_FixedSleep_1.xml",
            "--scheduling_class",       "fixed",
            "--working_directory",       "${HOME}",
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
        // Do "normal" tests
        //
        try {
            runNormal();
        } catch ( Exception e ) {
            System.out.println("Normal test failed.");
        }
        

        //
        // Do ping-only tests
        //
        try {
            runPingOnly();
        } catch ( Exception e ) {
            System.out.println("Ping-only test failed.");
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

