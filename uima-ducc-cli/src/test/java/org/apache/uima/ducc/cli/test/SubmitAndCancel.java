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
import java.util.List;
import java.util.Properties;

import org.apache.uima.ducc.cli.DuccJobCancel;
import org.apache.uima.ducc.cli.DuccJobSubmit;
import org.apache.uima.ducc.cli.IDuccCallback;

public class SubmitAndCancel
    extends ATestDriver
{

    String[] submit_args = new String[] {
        "--driver_descriptor_CR",           "org.apache.uima.ducc.test.randomsleep.FixedSleepCR",
        "--driver_descriptor_CR_overrides", "jobfile=${DUCC_HOME}/examples/simple/1.inputs compression=10 error_rate=0.0",
        "--driver_jvm_args",                "-Xmx500M",
        
        "--process_descriptor_AE",          "org.apache.uima.ducc.test.randomsleep.FixedSleepAE",
        "--process_memory_size",            "2",
        "--classpath",                      "${DUCC_HOME}/lib/uima-ducc/examples/*:${DUCC_HOME}/apache-uima/lib/*",
        /* Use the following if testing changes in the cli & examples projects
        "--classpath",                      "../uima-ducc-cli/target/classes:../uima-ducc-examples/target/classes:${DUCC_HOME}/apache-uima/lib/*",
        */
        "--process_jvm_args",               "-Xmx100M ",
        "--process_thread_count",           "2",
        "--process_per_item_time_max",      "5",
        "--environment",                    "AE_INIT_TIME=5 AE_INIT_RANGE=5 INIT_ERROR=0 LD_LIBRARY_PATH=/yet/a/nother/dumb/path",
        "--process_deployments_max",        "999",
        
        "--scheduling_class",               "normal",  
    };
    List<String> failReasons = new ArrayList<String>();
    List<String> successReasons = new ArrayList<String>();

    SubmitAndCancel()
    {
    }

    //
    // establish the tests and the order of execution
    //
    public String[] testsToRun()
    {
        return new String[] {
            "Submit",
            "SubmitAndCancel",
            "WaitForCompletion",
            "AttachConsole",
            "ServiceDependency",
            "AllInOneLocal",
            "AllInOneRemote",
            "AllInOneBogus",
        };
    }

    // void fail(String testid, String reason)
    //     throws FailedTestException
    // {
    //     failReasons.add(testid + " - " + reason);
    //     System.out.println(testid + " - test failed: " + reason);
    // }

    // void success(String testid, String reason)
    // {
    //     successReasons.add(testid + " - " + reason);
    //     System.out.println(testid + " - test passed: " + reason);
    // }

    Properties mkproperties(String[] args)
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

    void testSubmit(String testid)
    	throws Exception
    {
        IDuccCallback cb = new MyCallback();
        DuccJobSubmit submit;

        submit = new DuccJobSubmit(submit_args, (IDuccCallback) cb);
        submit.setProperty("description", "Submit-And-Cancel job 1");
        System.out.println(testid + " ------------------------------ Submit with a callback ------------------------------");
        System.out.println(testid + " Console attached: " + submit.isConsoleAttached());
        if ( submit.execute() ) {
            success(testid, "Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
        } else {
            fail(testid, "Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
            return;
        }
    }

    void testSubmitAndCancel(String testid)
    	throws Exception
    {
        DuccJobSubmit submit;

        submit = new DuccJobSubmit(submit_args);
        submit.setProperty("description", "Submit-And-Cancel job 2");
        System.out.println(testid + " ------------------------------ Submit normal ------------------------------");
        System.out.println(testid + "Console attached: " + submit.isConsoleAttached());
        if ( submit.execute() ) {
            success(testid, "Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
        } else {
            fail(testid, "Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
            return;
        }
        
        Thread.sleep(10000);
        
        DuccJobCancel cancel = new DuccJobCancel(
            new String[] {
                "--id", "" + submit.getDuccId(),
            }
        );
        
        System.out.println(testid + "------------------------------ Cancel first job quickly ------------------------------");
        if ( cancel.execute() ) {
            success(testid, "Job " + submit.getDuccId() + " canceled, rc = " + cancel.getReturnCode());
        } else {
            fail(testid, "Job " + submit.getDuccId() + " cancel failed, rc = " + cancel.getReturnCode());
        }
        
    }

    void testWaitForCompletion(String testid)
        throws Exception
    {
        IDuccCallback cb = new MyCallback();
        DuccJobSubmit submit;

        cb = new MyCallback();         // insert a callback because the earlier one just tested the constructor
        // this time the callback will actually get called from the monitor
        submit = new DuccJobSubmit(submit_args, (IDuccCallback) cb);
        submit.setProperty("wait_for_completion", "true");
        submit.setProperty("description", "Submit-And-Cancel job 3");
        System.out.println(testid + " ------------------------------ Submit and wait for completion ------------------------------");
        if ( submit.execute() ) {
            success(testid, "Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
        } else {
            fail(testid, "Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
        }
    }

    void testAttachConsole(String testid)
        throws Exception
    {
        DuccJobSubmit submit;
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
        System.out.println(testid + " ------------------------------ Submit with attached console ------------------------------");
        System.out.println(testid + " Console attached: " + submit.isConsoleAttached());
        if ( submit.execute() ) {
            success(testid, "Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
        } else {
            fail(testid, "Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
        }
    }


    void testServiceDependency(String testid)
        throws Exception
    {
        IDuccCallback cb = new MyCallback();
        DuccJobSubmit submit;
        System.out.println(testid + " ------------------------------ Submit with service dependency ------------------------------");
        System.out.println(testid + "   ------ The job may fail if the service isn't registered -- that is OK ------------------");
        Properties props = mkproperties(submit_args);      // test the constructor, plus easier to update
        props.setProperty("service_dependency", "UIMA-AS:FixedSleepAE_1:tcp://localhost:61617");
        props.setProperty("wait_for_completion", "true");

        cb = new MyCallback(); // why not, lets go nuts
        submit = new DuccJobSubmit(props, (IDuccCallback) cb);

        System.out.println(testid + " Console attached: " + submit.isConsoleAttached());
        if ( submit.execute() ) {
            success(testid, "Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
        } else {
            fail(testid, "Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
        }

    }            

    void testAllInOneLocal(String testid)
        throws Exception
    {
        IDuccCallback cb = new MyCallback();
        DuccJobSubmit submit;

        // Now some all-in-one variants - local
        ArrayList<String> arglist = new ArrayList<String>();
        for ( String s : submit_args ) {
            arglist.add(s);
        }
        arglist.add("--all_in_one");
        arglist.add("local");
        arglist.add("--description");
        arglist.add("Submit-And-Cancel job 5");

        try {
            cb = new MyCallback(); // why not, lets go nuts
            submit = new DuccJobSubmit(arglist, (IDuccCallback) cb);
            System.out.println(testid + " ------------------------------ Submit all_in_one local ------------------------------");
            System.out.println(testid + " Console attached: " + submit.isConsoleAttached());
            if ( submit.execute() ) {
                int rc = submit.getReturnCode();
                if (rc == 0) {
                    success(testid, "All-in-one local job completed, rc = 0");
                } else {
                    fail(testid, "All-in-one local job completed, rc = " + rc);
                }
            } else {
                fail(testid, "All-in-one local job not submitted, rc = " + submit.getReturnCode());
            }
        } catch ( Throwable t ) {
              fail(testid, "All-In-One local Job failed with exception " + t.toString());
              t.printStackTrace();
        }
    }

    void testAllInOneRemote(String testid)
        throws Exception
    {
        IDuccCallback cb = new MyCallback();
        DuccJobSubmit submit;

        Properties props = mkproperties(submit_args);      // test the constructor, plus easier to update
        props.setProperty("all_in_one", "remote");
        props.setProperty("scheduling_class", "fixed");
        props.setProperty("description", "Submit-And-Cancel job 6");

        // for ( Object k : props.keySet() ) {
        //     System.out.println("Props: k=" + k + " v=" + props.get(k));
        // }

        try {
            cb = new MyCallback(); // why not, lets go nuts
            submit = new DuccJobSubmit(props, (IDuccCallback) cb);
            System.out.println(testid + " ------------------------------ Submit all_in_one remote ------------------------------");
            System.out.println(testid + " Console attached: " + submit.isConsoleAttached());
            if ( submit.execute() ) {
                success(testid, "Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
            } else {
                fail(testid, "Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
            }
        } catch ( Throwable e ) {
            fail(testid, "All-In-One Remote Job failed with exception " + e.toString());
            e.printStackTrace();
        }
    }

    void testAllInOneBogus(String testid)
        throws Exception
    {
        IDuccCallback cb = new MyCallback();
        DuccJobSubmit submit = null;

        Properties props = mkproperties(submit_args);      // test the constructor, plus easier to update        
        props.setProperty("all_in_one", "bogus");
        props.setProperty("description", "Submit-And-Cancel job 7");
        props.setProperty("scheduling_class", "fixed");

        cb = new MyCallback(); // why not, lets go nuts
        try {
            System.out.println(testid + " ------------------------------ Submit all_in_one bogus, should fail ------------------------------");
            submit = new DuccJobSubmit(props, (IDuccCallback) cb);
            System.out.println(testid + " Console attached: " + submit.isConsoleAttached());
            if ( submit.execute() ) {
                fail(testid, "Job " + submit.getDuccId() + " submitted, rc = " + submit.getReturnCode());
            } else {
                success(testid, "Job " + submit.getDuccId() + " not submitted, rc = " + submit.getReturnCode());
            }
        } catch (Exception e) {
            success(testid, "Job " + submit.getDuccId() + " could not be submitted.");
            return;
        }

        success(testid, "Job " + submit.getDuccId() + " was submited!");
    }
    
    public static void main(String[] args)
    {
        try {
            SubmitAndCancel tester = new SubmitAndCancel();
            tester.runTests();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
    class MyCallback
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
