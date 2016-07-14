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

//import org.apache.commons.cli.HelpFormatter;
//import org.apache.commons.cli.Options;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.common.CancelReasons.CancelReason;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;

public class DuccReservationMonitor extends DuccMonitor implements IDuccMonitor {

	public static String servlet = "/ducc-servlet/proxy-reservation-status";
	
	public DuccReservationMonitor() {
		super(DuccContext.Reservation, true);
	}
	
	public DuccReservationMonitor(IDuccCallback messageProcessor) {
		super(DuccContext.Reservation, true, messageProcessor);
	}
	
	private DuccReservationMonitor(String uniqueSignature) {
		super(DuccContext.Reservation, false);
	}

	public static void main(String[] args) {
		int code = RC_FAILURE;
		try {
			String unique = null;
			DuccReservationMonitor reservationMonotor = new DuccReservationMonitor(unique);
			code = reservationMonotor.run(args);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(code);
	}
	
	// @Override
	// public void help(IUiOption[] options) 
    // {
    //     command_line.formatHelp(this.getClass().getName());
	// 	return;
	// }
	
	@Override
	public void cancel() {
		try {
       		ArrayList<String> arrayList = new ArrayList<String>();
       		arrayList.add("--"+UiOption.JobId.pname());
       		arrayList.add(getId());
       		arrayList.add("--"+UiOption.Reason.pname());
       		String reason = CancelReason.MonitorEnded.getText();
       		arrayList.add("\""+reason+"\"");
       		String[] argList = arrayList.toArray(new String[0]);
    		DuccReservationCancel reservationCancel = new DuccReservationCancel(argList);
    		boolean retVal = reservationCancel.execute();
    		debug("cancel rc:"+retVal);
    	} catch (Exception e) {
    		messageProcessor.status(e.toString());
    	}
	}

	@Override
	public String getUrl(String id) {
		String urlString = "http://"+getHost()+":"+getPort()+servlet+"?id="+id;
		debug(urlString);
		return urlString;
	}
}
