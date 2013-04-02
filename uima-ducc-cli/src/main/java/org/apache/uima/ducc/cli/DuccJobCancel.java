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

import org.apache.uima.ducc.transport.event.CancelJobDuccEvent;
import org.apache.uima.ducc.transport.event.CancelJobReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;

/**
 * Cancel a DUCC job
 */

public class DuccJobCancel 
    extends CliBase
{
	
    JobRequestProperties jobRequestProperties = new JobRequestProperties();
    static String or_port = "ducc.orchestrator.http.port";
    static String or_host = "ducc.orchestrator.node";

    long canceledPid = -1;
    String responseMessage = null;

    UiOption[] opts = new UiOption []
    {
        UiOption.Help,
        UiOption.Debug, 

        UiOption.JobId,
        UiOption.DjPid,
        UiOption.Reason,  
        UiOption.RoleAdministrator,
    };

	
	public DuccJobCancel(String [] args) 
        throws Exception
    {
        init(this.getClass().getName(), opts, args, jobRequestProperties, or_host, or_port, "or", null);
	}

	public DuccJobCancel(List<String> args) 
        throws Exception
    {
        String[] arg_array = args.toArray(new String[args.size()]);
        init(this.getClass().getName(), opts, arg_array, jobRequestProperties, or_host, or_port, "or", null);
	}

	public DuccJobCancel(Properties props) 
        throws Exception
    {
        for ( Object k : props.keySet() ) {      
            Object v = props.get(k);
            jobRequestProperties.put(k, v);
        }
        init(this.getClass().getName(), opts, null, jobRequestProperties, or_host, or_port, "or", null, null);
	}

	public long getCanceledPid()
	{
		return canceledPid;
	}
	public String getResponseMessage()
	{
		return responseMessage;
	}
		
	public boolean execute() 
        throws Exception 
    {

        CancelJobDuccEvent      cancelJobDuccEvent      = new CancelJobDuccEvent(jobRequestProperties);
        CancelJobReplyDuccEvent cancelJobReplyDuccEvent = null;
        try {
            cancelJobReplyDuccEvent = (CancelJobReplyDuccEvent) dispatcher.dispatchAndWaitForDuccReply(cancelJobDuccEvent);
        } catch (Exception e) {
            message("Job not submitted:", e.getMessage());
            return false;
        } finally {
            dispatcher.close();
        }
			
        /*
         * process reply
         */
    	boolean rc = extractReply(cancelJobReplyDuccEvent);
        
        String dpId = cancelJobReplyDuccEvent.getProperties().getProperty(UiOption.DjPid.pname());
        if(dpId != null) {
            canceledPid = Long.parseLong(dpId);
        }
    	
    	responseMessage = cancelJobReplyDuccEvent.getProperties().getProperty(UiOption.Message.pname());

        // need : getResponseMessage
        //      : canceled Pids
        //      : getDuccId
    	// duccMessageProcessor.out("Job"+" "+jobId+" "+msg);
		return rc;
	}
	
	public static void main(String[] args) {
		try {
			DuccJobCancel djc = new DuccJobCancel(args);
			boolean rc = djc.execute();

            long id = djc.getDuccId();
            String msg = djc.getResponseMessage();
            long dpid = djc .getCanceledPid();

            if ( dpid == -1 ) {
                System.out.println("Job " + id + " " + msg);
            } else {
                System.out.println("Process " + id + "." + dpid + " " + msg);
            }

            System.exit(rc ? 0 : 1);
		} catch (Exception e) {
            System.out.println("Cannot cancel: " + e.getMessage());
            System.exit(1);
		}
	}
	
}
