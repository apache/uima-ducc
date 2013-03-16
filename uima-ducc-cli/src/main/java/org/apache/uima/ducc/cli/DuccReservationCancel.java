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

import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;

/**
 * Cancel a DUCC reservation
 */

public class DuccReservationCancel 
    extends CliBase
{

    ReservationRequestProperties requestProperties = new ReservationRequestProperties();	
    static String or_port = "ducc.orchestrator.http.port";
    static String or_host = "ducc.orchestrator.node";

    long canceledPid = -1;
    String responseMessage = null;

    UiOption[] opts = new UiOption []
    {
        UiOption.Help,
        UiOption.Debug, 

        UiOption.JobId,
        UiOption.RoleAdministrator,
    };

	
	public DuccReservationCancel(String [] args) 
        throws Exception
    {
        init(this.getClass().getName(), opts, args, requestProperties, or_host, or_port, "or");
	}

	public DuccReservationCancel(List<String> args) 
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        init(this.getClass().getName(), opts, arg_array, requestProperties, or_host, or_port, "or");
	}

	public DuccReservationCancel(Properties props) 
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

        CancelReservationDuccEvent      cancelReservationDuccEvent      = new CancelReservationDuccEvent(requestProperties);
        CancelReservationReplyDuccEvent cancelReservationReplyDuccEvent = null;
        try {
            cancelReservationReplyDuccEvent = (CancelReservationReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(cancelReservationDuccEvent);
        } catch (Exception e) {
            addError("Job not submitted: " + e.getMessage());
            return false;
        } finally {
            dispatcher.close();
        }
			
        /*
         * process reply
         */
    	boolean rc = extractReply(cancelReservationReplyDuccEvent);            	
    	responseMessage = cancelReservationReplyDuccEvent.getProperties().getProperty(UiOption.Message.pname());
		return rc;
	}
	
	public static void main(String[] args) {
		try {
			DuccReservationCancel dsc = new DuccReservationCancel(args);
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
            System.out.println("Reservation " + id + " " + msg);
            System.exit(rc ? 0 : 1);

		} catch (Exception e) {
            System.out.println("Cannot cancel: " + e.getMessage());
            System.exit(1);
		}
	}
	
}
