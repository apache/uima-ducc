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
package org.apache.uima.ducc.transport.dispatcher;


import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.transport.event.DuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobReplyDuccEvent;
/**
 * Implementation of the HTTP based dispatcher. Uses commons HTTPClient for 
 * messaging. The body of each message is converted to a String (xml format).
 * Default socket timeout is 30 minutes.
 * 
 */
/**
 * Implementation of the HTTP based dispatcher. Uses commons HTTPClient for 
 * messaging. The body of each message is converted to a String (xml format).
 * Default socket timeout is 30 minutes.
 * 
 */
public class DuccEventHttpDispatcher 
    extends BaseHttpDispatcher
    implements IDuccEventDispatcher
{   
    String targetEndpoint;
    
    int socketTimeout = 0;  // no timeout

    public DuccEventHttpDispatcher( String targetEndpoint ) 
        throws Exception 
    {
        super(targetEndpoint, -1);
    }
        
    public DuccEventHttpDispatcher( String targetEndpoint, int timeout)
        throws Exception 
    {
        super(targetEndpoint, timeout);
    }

    String toXml(Object ev)
        throws Exception
    {        
    	return XStreamUtils.marshall(ev);
    }

    Object fromXml(String str)
        throws Exception
    {        
    	return XStreamUtils.unmarshall(str);
    }

    public static void main(String[] args) {
        try {
            DuccEventHttpDispatcher dispatcher = 
                new DuccEventHttpDispatcher("http://"+args[0]+":19988/or",1000*4);
            SubmitJobDuccEvent duccEvent = new SubmitJobDuccEvent(null, 1);
            DuccEvent event = dispatcher.dispatchAndWaitForDuccReply(duccEvent);
            if ( event instanceof SubmitJobReplyDuccEvent ) {
                System.out.println("Client received SubmitJobReplyDuccEvent");
            }
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
    
}
