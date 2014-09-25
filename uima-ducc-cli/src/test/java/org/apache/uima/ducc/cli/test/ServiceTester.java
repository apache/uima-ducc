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
import java.util.List;
import java.util.Map;

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

    static String[] props = {
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

    /**
     * Must look into ducc.properties to find the broker.
     */
    static String mkBrokerString(String ducc_home)
    	throws Exception
    {
        DuccProperties props = new DuccProperties();
        props.load(new FileInputStream(ducc_home + "/resources/ducc.properties"));
        String proto = props.getStringProperty("ducc.broker.protocol");
        String host = props.getStringProperty("ducc.broker.hostname");
        String port = props.getStringProperty("ducc.broker.port");
        return proto + "://" + host + ":" + port;
    }

    
    // replicates ServiceDescription toString in order to test the interfaces
     static String serviceToString(IServiceDescription desc)
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

    public static List<IServiceDescription> query()
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

    static IServiceDescription getServiceDescription(String id)
    {
        try {
			DuccServiceApi api = new DuccServiceApi(null);
			IServiceReply qreply;
			List<IServiceDescription> service_list;
			
			qreply = api.query(new String[] {"--query", id});      // general service reply
			service_list = qreply.getServiceDescriptions();        // list of service descriptions
			return service_list.get(0);                            // failure here is test failure, no need to check too carefully
		} catch (Exception e) {
            System.out.println("Query failed, terminating test.");
			e.printStackTrace();
            System.exit(1);
		}
        return null; // impossible, stupid eclipse
    }

    static void waitForState(String desired_state, String id)
    {
        try {

			IServiceDescription desc = getServiceDescription(id);
			System.out.println("Waiting for service " + id + " to reach state " + desired_state);
			String state = desc.getServiceState().toString();
			while ( !state.equals(desired_state) ) {
			    System.out.println(" ... " + state);
			    Thread.sleep(5000);
			    desc = getServiceDescription(id);
			    state = desc.getServiceState().toString();
			}
		} catch (Exception e) {
            System.out.println("Query failed, terminating test.");
            System.exit(1);
			e.printStackTrace();
		}
    }


    static void waitForStartState(String desired_state, String id)
    {
        try {

            do {
                IServiceDescription desc = getServiceDescription(id);
                List<ADuccId> implementors = desc.getImplementors();
                System.out.println("Waiting for service " + id + " to reach start state " + desired_state);
                System.out.println(" ... autostart " + desc.isAutostart() + " reference " + desc.isReferenceStart() + " n_implementors " + implementors.size());

                if ( desc.isAutostart()      && desired_state.equals("autostart") ) return;
                if ( desc.isReferenceStart() && desired_state.equals("reference") ) return;

                if ( !desc.isAutostart() && (!desc.isReferenceStart()) ) {
                    if ( implementors.size() >  0 && desired_state.equals("manual") ) return;
                    if ( implementors.size() == 0 && desired_state.equals("stopped") ) return;
                }
                
                System.out.println(" ... autostart " + desc.isAutostart() + " reference " + desc.isReferenceStart() + " n_implementors " + implementors.size());
			    Thread.sleep(5000);
			} while ( true );
		} catch (Exception e) {
            System.out.println("Query failed, terminating test.");
            System.exit(1);
			e.printStackTrace();
		}
    }
    
    public static void main(String[] args)
    {
        DuccServiceApi api;
        IDuccCallback cb = new MyCallback();
        IServiceReply reply;

        try {
            // Need broker location
            String ducc_home = System.getProperty("DUCC_HOME");
            if ( ducc_home == null ) {
                throw new IllegalArgumentException("DUCC_HOME must be set into system properties.");
            }
            props[3] = "-Xmx100M -DdefaultBrokerURL=" + mkBrokerString(ducc_home);  // any failure, test setup is broken, don't bother with checking
                                                                          // we'll crash soon enough if its broken
            
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
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    static class MyCallback
        implements IDuccCallback
    {
        public void console(int pnum, String  msg)
        {
            System.out.println("---> " + pnum + " " + msg);
        }

        public void status(String msg)
        {
            System.out.println("---> " +msg);
        }
    }

}

