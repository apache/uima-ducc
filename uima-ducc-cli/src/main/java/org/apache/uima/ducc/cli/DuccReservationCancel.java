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

    long canceledPid = -1;
    String responseMessage = null;

    UiOption[] opts = new UiOption []
    {
        UiOption.Help,
        UiOption.Debug, 

        UiOption.ReservationId,
        UiOption.Reason,
        UiOption.RoleAdministrator,
    };

    /**
     * @param args Array of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_UNRESERVE">DUCC CLI reference.</a>
     */	
	public DuccReservationCancel(String [] args) 
        throws Exception
    {
        init (this.getClass().getName(), opts, args, requestProperties, null);
	}

    /**
     * @param args List of string arguments as described in the 
     *      <a href="/doc/duccbook.html#DUCC_CLI_UNRESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationCancel(List<String> args) 
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        init (this.getClass().getName(), opts, arg_array, requestProperties, null);
	}

    /**
     * @param props Properties file of arguments, as described in the
     *      <a href="/doc/duccbook.html#DUCC_CLI_UNRESERVE">DUCC CLI reference.</a>
     */
	public DuccReservationCancel(Properties props) 
        throws Exception
    {
        init (this.getClass().getName(), opts, props, requestProperties, null);
	}

    /**
     * Return the DUCC Orchestrator message, if any, pertaining to the cancelation.
     *
     * @return Return any message associated with the cancelation.
     */
	public String getResponseMessage()
	{
		return responseMessage;
	}

    /**
     * Execute collects the parameters for reservation cancelation and sends them to the DUCC Orchestrator
     * to effect the cancelation.
     *
     * @return True if the orchestrator accepts the reservation cancelation.
     */
	public boolean execute() 
        throws Exception 
    {

        CancelReservationDuccEvent      cancelReservationDuccEvent      = new CancelReservationDuccEvent(requestProperties, CliVersion.getVersion());
        CancelReservationReplyDuccEvent cancelReservationReplyDuccEvent = null;
        try {
            cancelReservationReplyDuccEvent = (CancelReservationReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(cancelReservationDuccEvent);
        } catch (Exception e) {
            message("Cancel not submitted:", e.getMessage());
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
	
    /**
     * Main method, as used by the executable jar or direct java invocation.
     * @param args arguments as described in the <a href="/doc/duccbook.html#DUCC_CLI_UNRESERVE">DUCC CLI reference.</a>
     */
	public static void main(String[] args) {
		try {
			DuccReservationCancel dsc = new DuccReservationCancel(args);
			boolean rc = dsc.execute(); 

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
