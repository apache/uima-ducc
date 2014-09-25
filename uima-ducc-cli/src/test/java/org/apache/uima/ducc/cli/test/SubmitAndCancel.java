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

import java.util.ArrayList;
import java.util.Properties;

import org.apache.uima.ducc.cli.DuccJobCancel;
import org.apache.uima.ducc.cli.DuccJobSubmit;
import org.apache.uima.ducc.cli.IDuccCallback;

public class SubmitAndCancel
{

    static Properties mkproperties(String[] args)
    {
        Properties props = new Properties();
        if ( args.length % 2 != 0 ) {
            throw new IllegalStateException("Job arguments must be paired.  Found " + args.length + " arguments.");
        }

        for (int i = 0; i < args.length; i += 2) {
            String k = args[i].substring(2);
            String v = args[i+1];
            props.setProperty(k, v);
        }
        return props;
    }

    public static void main(String[] args) 
    {
        String[] submit_args = new String[] {
            "--driver_descriptor_CR",           "org.apache.uima.ducc.test.randomsleep.FixedSleepCR",
            "--driver_descriptor_CR_overrides", "jobfile=${DUCC_HOME}/examples/simple/1.inputs compression=10 error_rate=0.0",
            "--driver_jvm_args",                "-Xmx500M",
            
            "--process_descriptor_AE",          "org.apache.uima.ducc.test.randomsleep.FixedSleepAE",
            "--process_memory_size",            "2",
            "--classpath",                      "${DUCC_HOME}/lib/uima-ducc/examples/*",
            "--process_jvm_args",               "-Xmx100M ",
            "--process_thread_count",           "2",
            "--process_per_item_time_max",      "5",
            "--environment",                    "AE_INIT_TIME=5 AE_INIT_RANGE=5 INIT_ERROR=0 LD_LIBRARY_PATH=/yet/a/nother/dumb/path",
            "--process_deployments_max",        "999",
            
            "--scheduling_class",               "normal",  
        };

        try {
            DuccJobSubmit submit;

            IDuccCallback cb = new MyCallback();
            submit = new DuccJobSubmit(submit_args, (IDuccCallback) cb);
            submit.setProperty("description", "Submit-And-Cancel job 1");
            System.out.println("------------------------------ Submit with a callback ------------------------------");
            System.out.println("Console attached: " + submit.isConsoleAttached());
			if ( submit.execute() ) {
                System.out.println("Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
            } else {
                System.out.println("Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
                return;
            }

            submit = new DuccJobSubmit(submit_args);
            submit.setProperty("description", "Submit-And-Cancel job 2");
            System.out.println("------------------------------ Submit normal ------------------------------");
            System.out.println("Console attached: " + submit.isConsoleAttached());
			if ( submit.execute() ) {
                System.out.println("Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
            } else {
                System.out.println("Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
                return;
            }

            Thread.sleep(10000);

            DuccJobCancel cancel = new DuccJobCancel(
               new String[] {
                   "--id", "" + submit.getDuccId(),
               }
            );

            System.out.println("------------------------------ Cancel first job quickly ------------------------------");
            if ( cancel.execute() ) {
                System.out.println("Job " + submit.getDuccId() + " canceled, rc = " + cancel.getReturnCode());
            } else {
                System.out.println("Job " + submit.getDuccId() + " cancel failed, rc = " + cancel.getReturnCode());
            }

            cb = new MyCallback();         // insert a callback because the earlier one just tested the constructor
                                           // this time the callback will actually get called from the monitor
            submit = new DuccJobSubmit(submit_args, (IDuccCallback) cb);
            submit.setProperty("wait_for_completion", "true");
            submit.setProperty("description", "Submit-And-Cancel job 3");
            System.out.println("------------------------------ Submit and wait for completion ------------------------------");
			if ( submit.execute() ) {
                System.out.println("Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
            } else {
                System.out.println("Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
                return;
            }

            // setProperty is broken for many things including attach_console. Copy the parms to a list and add
            // attach console.
            ArrayList<String> arglist = new ArrayList<String>();      // test ArrayList constructor
            for ( String s : submit_args ) {
                arglist.add(s);
            }
            arglist.add("--attach_console");
            arglist.add("--description");
            arglist.add("Submit-And-Cancel job 4");
            

			submit = new DuccJobSubmit(arglist);
            System.out.println("------------------------------ Submit with attached console ------------------------------");
            System.out.println("Console attached: " + submit.isConsoleAttached());
			if ( submit.execute() ) {
                System.out.println("Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
            } else {
                System.out.println("Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
                return;
            }
            

            // Now some all-in-one variants - local
            arglist = new ArrayList<String>();
            for ( String s : submit_args ) {
                arglist.add(s);
            }
            arglist.add("--all_in_one");
            arglist.add("local");
            arglist.add("--description");
            arglist.add("Submit-And-Cancel job 5");

            cb = new MyCallback(); // why not, lets go nuts
            submit = new DuccJobSubmit(arglist, (IDuccCallback) cb);
            System.out.println("------------------------------ Submit all_in_one local ------------------------------");
            System.out.println("Console attached: " + submit.isConsoleAttached());
			if ( submit.execute() ) {
                System.out.println("Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
            } else {
                System.out.println("Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
                return;
            }

            Properties props = mkproperties(submit_args);      // test the constructor, plus easier to update
            props.setProperty("all_in_one", "remote");
            props.setProperty("scheduling_class", "fixed");
            props.setProperty("description", "Submit-And-Cancel job 6");

            cb = new MyCallback(); // why not, lets go nuts
            submit = new DuccJobSubmit(props, (IDuccCallback) cb);
            System.out.println("------------------------------ Submit all_in_one remote ------------------------------");
            System.out.println("Console attached: " + submit.isConsoleAttached());
			if ( submit.execute() ) {
                System.out.println("Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
            } else {
                System.out.println("Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
                return;
            }

            props.setProperty("all_in_one", "bogus");
            props.setProperty("description", "Submit-And-Cancel job 7");
            props.setProperty("scheduling_class", "fixed");

            cb = new MyCallback(); // why not, lets go nuts
            try {
                System.out.println("------------------------------ Submit all_in_one bogus, should fail ------------------------------");
                System.out.println("Console attached: " + submit.isConsoleAttached());
                submit = new DuccJobSubmit(props, (IDuccCallback) cb);
                if ( submit.execute() ) {
                    System.out.println("Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
                } else {
                    System.out.println("Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
                    return;
                }
            } catch (Exception e) {
                System.out.println(" ... Expected failure ...");
                e.printStackTrace();
            }

            // Must see this, otherwise something is crashing that we didn't expect
            System.out.println("------------------------------ Submit Test Ends ------------------------------");

		} catch (Exception e) {
			// TODO Auto-generated catch block
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
