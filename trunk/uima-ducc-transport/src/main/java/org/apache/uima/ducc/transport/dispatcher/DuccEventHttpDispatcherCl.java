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
public class DuccEventHttpDispatcherCl 
    extends BaseHttpDispatcher
    implements IDuccEventDispatcher {
    
    private ClassManager classManager = null;

    String[] classpath = {
        "lib/apache-camel/xstream*",
        "lib/google-gson/gson*",
    };        

    int socketTimeout = 0;  // no timeout

    public DuccEventHttpDispatcherCl( String targetEndpoint ) 
        throws Exception 
    {
        this(targetEndpoint, -1);
    }
    
    public DuccEventHttpDispatcherCl( String targetEndpoint, int timeout ) 
        throws Exception 
    {
        super(targetEndpoint, timeout);
        classManager = new ClassManager(classpath);
    }

    String toXml(Object ev)
        throws Exception
    {        
        //  DomDriver dd = new DomDriver();

        Object dd_obj = classManager.construct("com.thoughtworks.xstream.io.xml.DomDriver", new Object[] {null});

        //    XStream xStream = new XStream(dd);
        Object   xStream_obj = classManager.construct("com.thoughtworks.xstream.XStream", new Object[] {dd_obj});

        //    return xStream.toXML(ev);
        return (String) classManager.invoke(xStream_obj, "toXML", new Object[] {ev});
    }

    Object fromXml(String str)
        throws Exception
    {        
        //  DomDriver dd = new DomDriver();
        Object   dd_obj = classManager.construct("com.thoughtworks.xstream.io.xml.DomDriver", new Object[] {null});

        //    XStream xStream = new XStream(dd);
        Object   xStream_obj = classManager.construct("com.thoughtworks.xstream.XStream", new Object[] {dd_obj});

        //    return xStream.fromXML(str);
        return classManager.invoke(xStream_obj, "fromXML", new Object[] {str});        
    }

    Object fromJson(String str, Class<?> cl)
        throws Exception
    {        
    	//  DomDriver dd = new Gson
        Object   gson_obj = classManager.construct("com.google.gson.Gson");

        //    return xStream.fromXML(targetToUnmarshall);
        return classManager.invoke(gson_obj, "fromJson", new Object[] {str, cl});        
    }


    public Object dispatchJson(Class<?> cl)
    	throws Exception
    {
        // no body, dispatch will use GET
        String response = dispatch(null, "application/json");  // rfc4627 - json mime type
        return fromJson(response, cl);
    }

    /**
     * Must call this if done using this class
     */
    public void close() {
        //if ( method != null ) {
        // method.releaseConnection();
        //}
    }
    public static void main(String[] args) {
        try {
            DuccEventHttpDispatcherCl dispatcher = 
                new DuccEventHttpDispatcherCl("http://"+args[0]+":19988/or",1000*4);
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
