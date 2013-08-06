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

package org.apache.uima.ducc.cli;

import java.util.ArrayList;

import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;


class MonitorListener
    implements Runnable
{
    CliBase        base  = null;
    long           jobid = 0;
    DuccProperties props = null;
    DuccContext    context = null;
    
    DuccMonitor monitor = null;

    MonitorListener(CliBase base, long jobid, DuccProperties props, DuccContext context)
    {
        this.base = base;
        this.jobid = jobid;
        this.props = props;
        this.context = context;
    }

    public void run()
    {
        int retVal = 0;
        try {
            ArrayList<String> arrayList = new ArrayList<String>();
            arrayList.add("--" + UiOption.JobId.pname());
            arrayList.add(""+jobid);

            if(props.containsKey(UiOption.Debug.pname())) {
                arrayList.add("--" + UiOption.Debug.pname());
            }

            if(props.containsKey(UiOption.Quiet.pname())) {
                arrayList.add("--" + UiOption.Quiet.pname());
            }
            
            if(props.containsKey(DuccUiConstants.name_timestamp)) {
                arrayList.add("--"+DuccUiConstants.name_timestamp);
            }

            if(props.containsKey(DuccUiConstants.name_submit_cancel_on_interrupt)) {
                arrayList.add("--"+DuccUiConstants.name_monitor_cancel_on_interrupt);
            }

            if(props.containsKey(DuccUiConstants.name_submit_cancel_job_on_interrupt)) {
                arrayList.add("--"+DuccUiConstants.name_monitor_cancel_job_on_interrupt);
            }

            if(props.containsKey(DuccUiConstants.name_submit_cancel_managed_reservation_on_interrupt)) {
                arrayList.add("--"+DuccUiConstants.name_monitor_cancel_managed_reservation_on_interrupt);
            }
            
            String[] argList = arrayList.toArray(new String[0]);
            switch(context) {
            case ManagedReservation:
            	monitor = new DuccManagedReservationMonitor(base.getCallback());
            	break;
            case Job:
            	monitor = new DuccJobMonitor(base.getCallback());
            	break;
            }
            retVal = monitor.run(argList);
        } catch (Exception e) {
            base.message("ERROR:" + e.toString());
            retVal = 999; // error rc's usually positive! // DuccUiConstants.ERROR;
        }
        base.monitorExits(retVal);
    }

    void shutdown()
    {
        if ( monitor != null ) {
            monitor.cancel();
        }
    }

}
