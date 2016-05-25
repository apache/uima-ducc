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

import java.util.Properties;

import org.apache.uima.ducc.cli.DuccManagedReservationCancel;
import org.apache.uima.ducc.cli.DuccManagedReservationSubmit;
import org.apache.uima.ducc.cli.IDuccCallback;

public class ManagedReserveAndCancel
    extends ATestDriver
{

    ManagedReserveAndCancel()
    {

    }

    public String[] testsToRun()
    {
        return new String[] {
            "ReserveSleep",
            "ReserveAndCancel",
            "AttachConsole",
            "WaitForCompletion",
            "Callback",
        };
    }

    void testReserveSleep(String testid)
        throws Exception
    {
        Properties reserve_props = new Properties();
        DuccManagedReservationSubmit reserve;

        reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid);
        reserve_props.setProperty("process_memory_size", "4");
        reserve_props.setProperty("process_executable", "/bin/sleep");
        reserve_props.setProperty("process_executable_args", "5");
        reserve_props.setProperty("scheduling_class", "fixed");



        reserve = new DuccManagedReservationSubmit(reserve_props);
        if ( reserve.execute() ) {
            success(testid, "Managed reservation", ""+reserve.getDuccId(), "submitted successfully, rc", ""+reserve.getReturnCode());
        } else {
            fail(testid, "Managed reservation submit failed, rc",  ""+reserve.getReturnCode());
        }
    }

    void testReserveAndCancel(String testid)
        throws Exception
    {
        Properties reserve_props = new Properties();
        DuccManagedReservationSubmit reserve;

        reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid);
        reserve_props.setProperty("process_executable_args", "30");
        reserve_props.setProperty("process_executable", "/bin/sleep");
        reserve = new DuccManagedReservationSubmit(reserve_props);
        if ( reserve.execute() ) {
            success(testid, "Managed reservation", ""+reserve.getDuccId(), "submitted successfully, rc =" + reserve.getReturnCode());
        } else {
            fail(testid, "Managed reservation submit failed, rc = " + reserve.getReturnCode());
        }

        Thread.sleep(10000);
        Properties cancel_props = new Properties();
        cancel_props.setProperty("id", ""+reserve.getDuccId());
        DuccManagedReservationCancel cancel = new DuccManagedReservationCancel(cancel_props);
        if ( cancel.execute() ) {
            success(testid, "Reservation", cancel.getDuccId(), "cancelled, rc =", reserve.getReturnCode(),  cancel.getResponseMessage());
        } else {                
            fail(testid, "Reservation", "cancel failed, rc =", reserve.getReturnCode(), cancel.getResponseMessage());
        }
    }

    void testAttachConsole(String testid)
        throws Exception
    {
        Properties reserve_props = new Properties();
        DuccManagedReservationSubmit reserve;

        reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid);
        reserve_props.setProperty("process_executable", "/bin/ls");
        reserve_props.setProperty("process_executable_args", "-atl ${HOME}");
        reserve_props.setProperty("attach_console", "true");
        reserve = new DuccManagedReservationSubmit(reserve_props);
        if ( reserve.execute() ) {
            success(testid, "Managed reservation", reserve.getDuccId(), "submitted successfully, rc =", reserve.getReturnCode());
        } else {
            fail(testid, "Managed reservation submit failed, rc = ", reserve.getReturnCode());
        }
    }

    void testWaitForCompletion(String testid)
        throws Exception
    {
        Properties reserve_props = new Properties();
        DuccManagedReservationSubmit reserve;

        reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid);
        reserve_props.setProperty("process_executable", "/bin/ls");
        reserve_props.setProperty("process_executable_args", "-atl ${HOME}");
        reserve_props.setProperty("attach_console", "true");
        reserve_props.setProperty("wait_for_completion", "true");
        reserve = new DuccManagedReservationSubmit(reserve_props);
        if ( reserve.execute() ) {
            success("Managed reservation", reserve.getDuccId(), "submitted successfully, rc =", reserve.getReturnCode());
        } else {
            fail("Managed reservation submit failed, rc = ", reserve.getReturnCode());
        }
    }

    void testCallback(String testid)
        throws Exception
    {
        Properties reserve_props = new Properties();
        DuccManagedReservationSubmit reserve;

        MyCallback cb = new MyCallback();
        reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid);
        reserve_props.setProperty("process_executable", "/bin/ls");
        reserve_props.setProperty("process_executable_args", "-atl ${HOME}");
        reserve_props.setProperty("attach_console", "true");
        reserve_props.setProperty("wait_for_completion", "true");
        reserve = new DuccManagedReservationSubmit(reserve_props, cb);
        if ( reserve.execute() ) {
            success("Managed reservation", reserve.getDuccId(), "submitted successfully, rc =", reserve.getReturnCode());
        } else {
            fail("Managed reservation submit failed, rc = ", reserve.getReturnCode());
        }
    }

    public static void main(String[] args) 
    {
        ManagedReserveAndCancel tester = new ManagedReserveAndCancel();
        tester.runTests();        
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
