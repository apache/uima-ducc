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

import java.util.List;
import java.util.Properties;

import org.apache.uima.ducc.transport.event.CancelServiceDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceReplyDuccEvent;
import org.apache.uima.ducc.transport.event.IDuccContext.DuccContext;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;

public class DuccManagedReservationCancel extends CliBase {

    JobRequestProperties requestProperties = new JobRequestProperties();
    static String or_port = "ducc.orchestrator.http.port";
    static String or_host = "ducc.orchestrator.node";

    long canceledPid = -1;
    String responseMessage = null;

    UiOption[] opts = new UiOption []
    {
        UiOption.Help,
        UiOption.Debug, 

        UiOption.ManagedReservationId,
        UiOption.Reason,  
        UiOption.RoleAdministrator,
    };

	public DuccManagedReservationCancel(String [] args) 
        throws Exception
    {
        init(this.getClass().getName(), opts, args, requestProperties, or_host, or_port, "or");
	}

	public DuccManagedReservationCancel(List<String> args) 
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        init(this.getClass().getName(), opts, arg_array, requestProperties, or_host, or_port, "or");
	}

	public DuccManagedReservationCancel(Properties props) 
        throws Exception
    {
        for ( Object k : props.keySet() ) {      
            Object v = props.get(k);
            requestProperties.put(k, v);
        }
        init(this.getClass().getName(), opts, null, requestProperties, or_host, or_port, "or");
	}

	public String getResponseMessage()
	{
		return responseMessage;
	}

	public boolean execute() 
        throws Exception 
    {
        CancelServiceDuccEvent      cancelServiceDuccEvent      = new CancelServiceDuccEvent(requestProperties, DuccContext.ManagedReservation);
        CancelServiceReplyDuccEvent cancelServiceReplyDuccEvent = null;
        try {
            cancelServiceReplyDuccEvent = (CancelServiceReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(cancelServiceDuccEvent);
        } catch (Exception e) {
            addError("Cancel not submitted: " + e.getMessage());
            return false;
        } finally {
            dispatcher.close();
        }
			
        /*
         * process reply
         */
    	boolean rc = extractReply(cancelServiceReplyDuccEvent);            	
    	responseMessage = cancelServiceReplyDuccEvent.getProperties().getProperty(UiOption.Message.pname());

		return rc;
	}
	
	public static void main(String[] args) {
		try {
			DuccManagedReservationCancel dsc = new DuccManagedReservationCancel(args);
			boolean rc = dsc.execute();

            // Fetch messages if any.  null means none
            String [] messages = dsc.getMessages();
            String [] warnings = dsc.getWarnings();
            String [] errors   = dsc.getErrors();

            if ( messages != null ) {
                for (String s : messages ) {
                    System.out.println(s);
                }
            }

            if ( warnings != null ) {
                for (String s : warnings ) {
                    System.out.println("WARN: " + s);
                }
            }

            if ( errors != null ) {
                for (String s : errors ) {
                    System.out.println("ERROR: " + s);
                }
            }

            long id = dsc.getDuccId();
            String msg = dsc.getResponseMessage();
            String dtype = "Managed Reservation";
            System.out.println(dtype + " " + id + " " + msg);
            System.exit(rc ? 0 : 1);

		} catch (Exception e) {
            System.out.println("Cannot cancel: " + e.getMessage());
            System.exit(1);
		}
	}
}
