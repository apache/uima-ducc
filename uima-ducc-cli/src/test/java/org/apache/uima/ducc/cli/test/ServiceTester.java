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
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
        "--service_linger",         "30000",
    };

    List<String> failReasons = new ArrayList<String>();
    List<String> successReasons = new ArrayList<String>();

    int timeout = 10;    // how many polls to allow before calling timeout
    ServiceTester()
    {
    }

    void fail(String testid, String reason)
        throws FailedTestException
    {
        failReasons.add(testid + ": " + reason);
        throw new FailedTestException(testid + " test failed: " + reason);
    }

    void fail(String testid, String reason, Exception e)
        throws FailedTestException
    {
        failReasons.add(testid + ": " + reason);
        throw new FailedTestException(testid + " test failed: " + reason, e);
    }

    void success(String testid, String reason)
    {
        successReasons.add(testid + ": " + reason);
        System.out.println(testid + " test passed: " + reason);
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
                
                if ( ++count > timeout ) {
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
        System.out.println("---------------------------------------- Ping-only ------------------------------");
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

        String service_id = registerService(service_props);
        if ( service_id == null ) {
            fail(testId, "Ping-only service test failed: can't register service.");
        }
        success(testId, "Ping-only Service is registered for manual start.");
                        
        if ( startService(service_id) ) {      // first start after registration
            success(testId, "Manual service start issued.");
        } else {
            fail(testId, "Manual service start failed.");
        }
        if ( waitForState("Available", service_id) ) {
            success(testId, "Service is manually started.");
        } else {
            fail(testId, "State Available timeout, state did not occur manually starting pinger.");
        }

        if ( stopService(service_id) ) {
            success(testId, "Manual service stop issued.");
        } else {
            fail(testId, "Manual service stop failed.");
        }
        if ( waitForState("Stopped", service_id) ) {
            success(testId, "Service is manually stopped");
        } else {
            fail(testId, "State Stopped timeout, state did not occur manually stopping pinger.");
        }
            
        if ( startService(service_id))  {                //r restart, verifies the previous stop cleaned up ok in SM
            success(testId, "Second Manual service start issued.");
        } else {
            fail(testId, "Second Manual service start failed.");
        }
        if ( waitForState("Available", service_id) ) {
            success(testId, "Service is manually restarted");
        } else {
            fail(testId, "State Stopped timeout, state did not occur manually restarting pinger.");
        }
            
        if ( stopService(service_id)) {     // stop again in prep for modify / autostart tests
            success(testId, "Manual service stop issued in preparation for autostart test.");
        } else {
            fail(testId, "Manual service stop failed preparing for autostart.");
        }
        if ( waitForState("Stopped", service_id) ) {
            success(testId, "Service is manually is stopped in preparation for autostart test.");
        } else {
            fail(testId, "State Stopped timeout, state did not occur manually stopping pinger in preparation for autostart test.");
        }

        if ( modifyService(service_id, "--autostart", "true")) {   // switch to autostart
            success(testId, "Modify --autostart=true issued.");
        } else {
            fail(testId, "Modify --autostart=true failed.");
        }  
        if ( waitForStartState("autostart", service_id) ) {
            success(testId, "Service is modified for autostart, waiting for it to start.");
        } else {
            fail(testId, "Service modify for --autostart failed.");
        }
            
        if ( enableService(service_id) )  {       // let it enable out of autostart
            success(testId, "Enable issued to stopped/autostart service.");
        } else {
            fail(testId, "Enable to stopped/autostart service failed.");
        }  
        if ( waitForState("Available", service_id) ) {
            success(testId, "Service is enabled and running with autostart enabled.");
        } else {
            fail(testId, "Service failed to start after autostart is enacled.");
        }

        if ( stopService(service_id) )  {          // once more, make sure it will stop out of autostart
            success(testId, "Second stop issued to autostarted service.");
        } else {
            fail(testId, "Second stop to autostarted service failed.");
        }    
        if ( waitForState("Stopped", service_id) ) {
            success(testId, "Service is stopped from autostart mode. Reenabling ...");
        } else {
            fail(testId, "State Stopped timeout, did not reach stop start after stop from autostart.");
        }

        if ( enableService(service_id) )  {          // once more, make sure it will restart out of autostart
            success(testId, "Second enable issued to autostarted service.");
        } else {
            fail(testId, "Second enable to autostarted service failed.");
        }    
        if ( waitForState("Available", service_id) ) {
            success(testId, "Service is enabled and running after stop from autostart.");
        } else {
            fail(testId, "Service did not restart after enable from autostart.");
        }

        if ( unregisterService(service_id) ) {  // finish the test
            success(testId, "Unregister issued.");
        } else {
            fail(testId, "Unregister failed.");
        }    
                
    }

    void exercisePingOnly()
    	throws Exception
    {
    	String testId = "Ping-Only";
        if ( startMr() ) {         // hangs until it's running
            success(testId, "Ping-only service test: anonymous service started as MR.");
        } else {
            fail(testId, "Cannot start anonymous service; ping-only test failed.");
        }
        
        String[] service_props = {
            "--description"              , "Custom Pinger",
            "--service_request_endpoint" , "CUSTOM:AnonymousPinger",
            "--service_ping_dolog"       , "true",
            "--service_ping_class"       , "org.apache.uima.ducc.test.service.AnonymousPinger",
            "--service_ping_classpath"   , ducc_home + "/lib/uima-ducc/examples/*",
            "--service_ping_timeout"     , "10000",
            "--service_ping_arguments"   , System.getenv("HOME") + "/ducc/logs/service/hostport",
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
        props[3] = "-Xmx100M -DdefaultBrokerURL=" + mkBrokerString(ducc_home);  // any failure, test setup is broken, don't bother with checking

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
        try {
            exercisePingOnly();
        } catch ( Exception e ) {
            System.out.println("Ping-only test failed.");
        }
            
           
        System.out.println("---------------------------------------- Register ----------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.register(props);
        String service_id = "<none>";
        if ( reply.getReturnCode() ) {
            service_id = Long.toString(reply.getId());
            System.out.println("Service " + service_id + " registered as " + reply.getEndpoint());
                
        } else {
            System.out.println("Service register failied: " + reply.getMessage() + " terminating test.");
            System.exit(1);
        }
        query();
        if (true) return;

        System.out.println("---------------------------------------- Start ----------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.start(new String[] {"--start", "" + service_id });
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " start requested " + reply.getEndpoint());
        } else {
            System.out.println("Service stop failied: " + reply.getMessage());
        }
            
        // Now wait until it's Available
        waitForState("Available", service_id);
            
        System.out.println("Service is started");
        query();

        System.out.println("---------------------------------------- Stop ----------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.stop(new String[] {"--stop", "" + service_id});
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " stop requested " + reply.getEndpoint());
        } else {
            System.out.println("Service stop failied: " + reply.getMessage());
        }
        waitForState("Stopped", service_id);
        System.out.println("Service is stopped");
        query();

        System.out.println("---------------------------------------- Enable ----------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.enable(new String[] {"--enable", "" + service_id});  // so disable has an effect
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " enable requested " + reply.getEndpoint());
        } else {
            System.out.println("Service enable failied: " + reply.getMessage());
        }
        query();
			

        System.out.println("---------------------------------------- Disable ----------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.disable(new String[] {"--disable", "" + service_id});
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " disable requested " + reply.getEndpoint());
        } else {
            System.out.println("Service disable failied: " + reply.getMessage());
        }
        query();
			
        System.out.println("---------------------------------------- Enable ----------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.enable(new String[] {"--enable", "" + service_id}); // again, so we can continue
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " enable requested " + reply.getEndpoint());
        } else {
            System.out.println("Service enable failied: " + reply.getMessage());
        }
        query();

        System.out.println("---------------------------------------- Modify ----------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.modify(new String[] {"--modify", "" + service_id, "--autostart", "true"}); // again, so we can continue
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " autostart modified " + reply.getEndpoint());
        } else {
            System.out.println("Service autostart modified: " + reply.getMessage());
        }
        // wait for it to show in the query
        waitForStartState("autostart", service_id);
        query();

        // Now wait until it's Available
        waitForState("Available", service_id);
            
        System.out.println("Service is started from autostart");
        query();


        System.out.println("---------------------------------------- Switch to reference start part 1, from autostart off -------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.modify(new String[] {"--modify", "" + service_id, "--autostart", "false"}); // again, so we can continue
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " autostart modified " + reply.getEndpoint());
        } else {
            System.out.println("Service autostart modify failedd: " + reply.getMessage());
        }
        waitForStartState("manual", service_id);
        query();

        api = new DuccServiceApi(cb);
        reply = api.observeReferences(new String[] {"--observe_references", "" + service_id}); // again, so we can continue
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " references being observed " + reply.getEndpoint());
        } else {
            System.out.println("Service cannot observe references:" + reply.getMessage());
        }
        waitForStartState("reference", service_id);
        query();

        waitForState("Stopped", service_id);            
        query();

        System.out.println("---------------------------------------- Switch to reference start part 2, from manual, then back ----------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.start(new String[] {"--start", "" + service_id}); // again, so we can continue
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " service starting" + reply.getEndpoint());
        } else {
            System.out.println("Service start failed: " + reply.getMessage());
        }
        waitForState("Available", service_id);
        query();

        api = new DuccServiceApi(cb);
        reply = api.observeReferences(new String[] {"--observe_references", "" + service_id}); // again, so we can continue
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " references being observed " + reply.getEndpoint());
        } else {
            System.out.println("Service cannot observe references:" + reply.getMessage());
        }
        waitForStartState("reference", service_id);
        query();

        api = new DuccServiceApi(cb);
        reply = api.ignoreReferences(new String[] {"--ignore_references", "" + service_id}); // again, so we can continue
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " references being ignored " + reply.getEndpoint());
        } else {
            System.out.println("Service cannot ignore references:" + reply.getMessage());
        }
        waitForStartState("manual", service_id);
        query();

            
        System.out.println("---------------------------------------- Test done, stopping service  ----------------------------------------");
        api = new DuccServiceApi(cb);
        reply = api.stop(new String[] {"--stop", "" + service_id});
        if ( reply.getReturnCode() ) {
            System.out.println("Service " + reply.getId() + " stop requested " + reply.getEndpoint());
        } else {
            System.out.println("Service stop failied: " + reply.getMessage());
        }
        waitForState("Stopped", service_id);
        query();
			
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
                        fail("Ping-Only test", "Managed reservation exited prematurely.");
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

