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
{

    public static void main(String[] args) 
    {

        try {
            Properties reserve_props = new Properties();
            int testid = 1;
            reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid++);
            reserve_props.setProperty("process_memory_size", "4");
            reserve_props.setProperty("process_executable", "/bin/sleep");
            reserve_props.setProperty("process_executable_args", "5");
            reserve_props.setProperty("scheduling_class", "fixed");

            DuccManagedReservationSubmit reserve;


            System.out.println("------------------------------ Managed Reservation Sleep Normal ------------------------------");
            reserve = new DuccManagedReservationSubmit(reserve_props);
            if ( reserve.execute() ) {
                System.out.println("Managed reservation " + reserve.getDuccId() + " submitted successfully, rc =" + reserve.getReturnCode());
            } else {
                System.out.println("Managed reservation submit failed, rc = " + reserve.getReturnCode());
            }

            System.out.println("------------------------------ Managed Reservation Sleep and Cancel ------------------------------");
            reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid++);
            reserve_props.setProperty("process_executable_args", "30");
            reserve = new DuccManagedReservationSubmit(reserve_props);
            if ( reserve.execute() ) {
                System.out.println("Managed reservation " + reserve.getDuccId() + " submitted successfully, rc =" + reserve.getReturnCode());
            } else {
                System.out.println("Managed reservation submit failed, rc = " + reserve.getReturnCode());
            }

            Thread.sleep(10000);
            Properties cancel_props = new Properties();
            cancel_props.setProperty("id", ""+reserve.getDuccId());
            DuccManagedReservationCancel cancel = new DuccManagedReservationCancel(cancel_props);
            if ( cancel.execute() ) {
                System.out.println("Reservation " + cancel.getDuccId() + " cancelled, rc = " + reserve.getReturnCode() + " " + cancel.getResponseMessage());
            } else {                
                System.out.println("Reservation " + cancel.getDuccId() + " cancel failed, rc = " + reserve.getReturnCode() + " " + cancel.getResponseMessage());
            }

            System.out.println("------------------------------ Managed Reservation ls with Attached Console ------------------------------");
            reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid++);
            reserve_props.setProperty("process_executable", "/bin/ls");
            reserve_props.setProperty("process_executable_args", "-atl ${HOME}*");
            reserve_props.setProperty("attach_console", "true");
            reserve = new DuccManagedReservationSubmit(reserve_props);
            if ( reserve.execute() ) {
                System.out.println("Managed reservation " + reserve.getDuccId() + " submitted successfully, rc =" + reserve.getReturnCode());
            } else {
                System.out.println("Managed reservation submit failed, rc = " + reserve.getReturnCode());
            }


            System.out.println("------------------------------ Managed Reservation ls Wait For Completion ------------------------------");
            reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid++);
            reserve_props.setProperty("process_executable", "/bin/ls");
            reserve_props.setProperty("process_executable_args", "-atl ${HOME}");
            reserve_props.setProperty("attach_console", "true");
            reserve_props.setProperty("wait_for_completion", "true");
            reserve = new DuccManagedReservationSubmit(reserve_props);
            if ( reserve.execute() ) {
                System.out.println("Managed reservation " + reserve.getDuccId() + " submitted successfully, rc =" + reserve.getReturnCode());
            } else {
                System.out.println("Managed reservation submit failed, rc = " + reserve.getReturnCode());
            }

            System.out.println("------------------------------ Managed Reservation ls With Callback ------------------------------");
            MyCallback cb = new MyCallback();
            reserve_props.setProperty("description", "Managed Reserve And Cancel " + testid++);
            reserve_props.setProperty("process_executable", "/bin/ls");
            reserve_props.setProperty("process_executable_args", "-atl ${HOME}");
            reserve_props.setProperty("attach_console", "true");
            reserve_props.setProperty("wait_for_completion", "true");
            reserve = new DuccManagedReservationSubmit(reserve_props, cb);
            if ( reserve.execute() ) {
                System.out.println("Managed reservation " + reserve.getDuccId() + " submitted successfully, rc =" + reserve.getReturnCode());
            } else {
                System.out.println("Managed reservation submit failed, rc = " + reserve.getReturnCode());
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
