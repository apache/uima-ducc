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


import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.uima.ducc.common.exception.DuccRuntimeException;
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
public class DuccEventHttpDispatcher {
    
    private ClassManager classManager = null;
    private String targetEndpoint;

    String[] classpath = {
        "lib/apache-camel/xstream*",
        "lib/google-gson/gson*",
    };        

    int socketTimeout = 0;  // no timeout

    public DuccEventHttpDispatcher( String targetEndpoint ) 
        throws Exception 
    {
        this(targetEndpoint, -1);
    }
    
    public DuccEventHttpDispatcher( String targetEndpoint, int timeout ) 
        throws Exception 
    {
        this.targetEndpoint = targetEndpoint;
        // System.out.println("ENDPOINT: " + targetEndpoint);
        if ( timeout == -1 ) {
            String st = System.getProperty("ducc.cli.httpclient.sotimeout");
            if (st != null ) {
                socketTimeout = Integer.parseInt(st);
            }
        } else {
            socketTimeout = timeout;
        }

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

    public String dispatch(String outgoing, String content_type)
        throws Exception
    {
 
        // String serBody = XStreamUtils.marshall(duccEvent);
        // String serBody = toXml(duccEvent);

        URL url = new URL(targetEndpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        if ( outgoing != null ) {             // if not null, we POST.  GET is default.
            //System.out.println(targetEndpoint + " using POST");
            //System.out.println("-------------- POST body ---------------");
            //System.out.println(outgoing);
            //System.out.println("----- end ---- POST body ---------------");
            conn.setRequestProperty("Content-Type", content_type);
            conn.setDoOutput(true);           // post
            // conn.setRequestProperty("Content-Type", "text/xml");
            OutputStream postout = conn.getOutputStream();
            postout.write(outgoing.getBytes());
            postout.close();
        } else {
            // System.out.println(targetEndpoint + " using GET");
        }

        int status = conn.getResponseCode();          // this will fire the connection

        if ( status == 200 ) {
            // System.out.println("Response headers:");
            //Map<String, List<String>> headers = conn.getHeaderFields();
            //for ( String s : headers.keySet() ) {
                //List<String> values = headers.get(s);
                // System.out.print("    " + s + ": ");
                // for ( String v : values ) System.out.print(v + " ");
                // System.out.println("\n");

                //if ( (s != null ) && s.equals("ContentType") ) { // nullkey! its the HTTP/1.1 200 OK header which is un-named
                    
                //}
            //}

            
            InputStream content = conn.getInputStream();
            StringBuffer sb = new StringBuffer();
            byte[] slice = new byte[4096];
            int bytes_read = 0;
            BufferedInputStream bis = new BufferedInputStream(content);
             while ( (bytes_read = bis.read(slice, 0, slice.length)) != -1 ) {
                sb.append(new String(slice, 0, bytes_read));
            }
            content.close();
            
            String response = sb.toString();
            // System.out.println("Response: " + response);

            return response;
        } else {
            String body = conn.getResponseMessage();   // getContent tends to throw if status is an error status and there is no body
            //System.out.println("BODY from failed HTTP request:");
            //System.out.println("-------------- POST body ---------------");
            //System.out.println(body);
            //System.out.println("----- fail --- POST body ---------------");

            throw new DuccRuntimeException("Ducc Unable to Process Request. Http Response Code: " + status + ". Ducc Service (OR) Returned Exception:",new Exception(body));
        }        
    }

    public DuccEvent dispatchAndWaitForDuccReply(DuccEvent duccEvent) 
        throws Exception
    {
        String serBody = null;
    	try{
    		serBody = toXml(duccEvent);
            String response =  dispatch(serBody, "text/xml");
            return (DuccEvent) fromXml(response);
    	} catch ( Throwable t ) { 
            t.printStackTrace(); 
        }
        return null;
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
