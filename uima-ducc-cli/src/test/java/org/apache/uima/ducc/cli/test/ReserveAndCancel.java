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
import org.apache.uima.ducc.cli.IDuccCallback;

public class ReserveAndCancel
{

    public static void main(String[] args) 
    {

        try {
            Properties reserve_props = new Properties();
            DuccReservationSubmit reserve;
            int testid = 1;

            System.out.println("------------------------------ Reserve Normal ------------------------------");
            reserve_props.setProperty("description", "Reserve And Cancel " + testid++);
            reserve_props.setProperty("instance_memory_size", "4");
            reserve_props.setProperty("number_of_instances", "2");
            reserve_props.setProperty("scheduling_class", "fixed");
            reserve = new DuccReservationSubmit(reserve_props);
            if ( reserve.execute() ) {
                System.out.println("Reservation " + reserve.getDuccId() + " successful, rc =" + reserve.getReturnCode() + ": " + reserve.getHostsAsString());
                String[] hosts = reserve.getHosts();
                System.out.println("" + hosts.length + " hosts assigned");
                if ( hosts.length > 0 ) {
                    for ( String h : reserve.getHosts() ) {
                        System.out.println("   " + h);
                    }
                }

            } else {
                System.out.println("Reservation failed, rc = " + reserve.getReturnCode());
            }


            Properties cancel_props = new Properties();
            cancel_props.setProperty("id", ""+reserve.getDuccId());
            DuccReservationCancel cancel = new DuccReservationCancel(cancel_props);
            if ( cancel.execute() ) {
                System.out.println("Reservation " + cancel.getDuccId() + " cancelled, rc = " + reserve.getReturnCode() + " " + cancel.getResponseMessage());
            } else {                
                System.out.println("Reservation " + cancel.getDuccId() + " cancel failed, rc = " + reserve.getReturnCode() + " " + cancel.getResponseMessage());
            }

            System.out.println("------------------------------ Reserve Fail ------------------------------");
            reserve_props.setProperty("description", "Reserve And Cancel " + testid++);
            reserve_props.setProperty("instance_memory_size", "99");
            reserve_props.setProperty("number_of_instances", "99");
            reserve_props.setProperty("scheduling_class", "fixed");
            reserve = new DuccReservationSubmit(reserve_props);
            if ( reserve.execute() ) {
                System.out.println("Reservation " + reserve.getDuccId() + " successful, rc =" + reserve.getReturnCode() + ": " + reserve.getHostsAsString());
                String[] hosts = reserve.getHosts();
                System.out.println("" + hosts.length + " hosts assigned");
                if ( hosts.length > 0 ) {
                    for ( String h : reserve.getHosts() ) {
                        System.out.println("   " + h);
                    }
                }
            } else {
                System.out.println("Reservation failed, rc = " + reserve.getReturnCode());
            }

            System.out.println("------------------------------ Cancel Fail ------------------------------");
            cancel_props.setProperty("id", "99999999999999");
            cancel = new DuccReservationCancel(cancel_props);
            if ( cancel.execute() ) {
                System.out.println("Reservation " + cancel.getDuccId() + " cancelled, rc = " + cancel.getReturnCode() + " " + cancel.getResponseMessage());
            } else {                
                System.out.println("Reservation " + cancel.getDuccId() + " cancel failed, rc = " + cancel.getReturnCode() + " " + cancel.getResponseMessage());
            }

            // Must see this, otherwise something is crashing that we didn't expect
            System.out.println("------------------------------ Reservation Test Ends ------------------------------");

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
