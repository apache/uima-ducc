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

import org.apache.uima.ducc.cli.DuccReservationCancel;
import org.apache.uima.ducc.cli.DuccReservationSubmit;

public class ReserveAndCancel
    extends ATestDriver
{

    String resid = null;

    ReserveAndCancel()
    {
    }

    //
    // establish the tests and the order of execution
    //
    public String[] testsToRun()
    {

        // if ( true ) return new int[] {5,};

        return new String[] {
            "Reserve",
            "Cancel",
            "ReserveFail",
            "CancelFail",
        };
    }
    
    void testReserve(String testid)
    	throws Exception
    {
        Properties reserve_props = new Properties();
        DuccReservationSubmit reserve;

        reserve_props.setProperty("description", "Reserve And Cancel");
        reserve_props.setProperty("instance_memory_size", "28");
        reserve_props.setProperty("number_of_instances", "1");
        reserve_props.setProperty("scheduling_class", "fixed");
        reserve = new DuccReservationSubmit(reserve_props);
        if ( reserve.execute() ) {
            resid = "" + reserve.getDuccId();            
            String host = reserve.getHost();
            success(testid, "Reservation", resid, "successful, rc =", ""+reserve.getReturnCode(), ":", host);               
        } else {
            fail(testid, "Reservation failed, rc = " + reserve.getReturnCode());
        }
    }

    void testCancel(String testid)
    	throws Exception
    {
        Properties cancel_props = new Properties();
        cancel_props.setProperty("id", resid);
        DuccReservationCancel cancel = new DuccReservationCancel(cancel_props);
        if ( cancel.execute() ) {
            success(testid, "Reservation " + ""+cancel.getDuccId() + " cancelled, rc = " + cancel.getReturnCode() + " " + cancel.getResponseMessage());
        } else {                
            fail(testid, "Reservation " + ""+cancel.getDuccId() + " cancel failed, rc = " + cancel.getReturnCode() + " " + cancel.getResponseMessage());
        }
    }

    void testReserveFail(String testid)
        throws Exception
    {
        Properties reserve_props = new Properties();
        DuccReservationSubmit reserve = null;

        reserve_props.setProperty("description", "Reserve And Cancel (fail)");
        reserve_props.setProperty("instance_memory_size", "99");
        reserve_props.setProperty("number_of_instances", "99");
        reserve_props.setProperty("scheduling_class", "fixed");
        reserve = new DuccReservationSubmit(reserve_props);
        if ( reserve.execute() ) {
            fail(testid, "Reservation " +""+ reserve.getDuccId() + " successful but should have failed, rc =" + ""+reserve.getReturnCode() + ": " + reserve.getHost());
            // designed to fail, if it doesn't we don't care about what is returned
        } else {
            success(testid, "Reservation failed as expected, rc = " + ""+reserve.getReturnCode());
        }
    }

    void testCancelFail(String testid)
        throws Exception
    {
        Properties cancel_props = new Properties();
        DuccReservationCancel cancel = null;

        System.out.println("------------------------------ Cancel Fail ------------------------------");
        cancel_props.setProperty("id", "9999999");
        cancel = new DuccReservationCancel(cancel_props);
        if ( cancel.execute() ) {
            fail(testid, "Reservation " + ""+cancel.getDuccId() + " cancelled but should have failed, rc = " + ""+cancel.getReturnCode() + " " + cancel.getResponseMessage());
        } else {                
            success(testid, "Reservation " + ""+cancel.getDuccId() + " cancel failed as expected, rc = " + ""+cancel.getReturnCode() + " " + cancel.getResponseMessage());
        }
        
    }
    
    public static void main(String[] args) 
    {

        ReserveAndCancel tester = new ReserveAndCancel();
        tester.runTests();
    }
    
}
