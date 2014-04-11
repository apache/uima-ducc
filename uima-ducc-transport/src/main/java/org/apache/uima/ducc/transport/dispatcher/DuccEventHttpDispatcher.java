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

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
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
public class DuccEventHttpDispatcher {
  private PostMethod method;
  private HttpClient httpclient;
  static int socketTimeout = 0;  // no timeout
  static {
    try {
      String st;
      if ( (st = System.getProperty("ducc.cli.httpclient.sotimeout") ) != null ) {
        socketTimeout = Integer.parseInt(st);
      }
    } catch( Exception e) {
      
    }
  }
  public DuccEventHttpDispatcher( String targetEndpoint ) throws Exception {
    this(targetEndpoint, socketTimeout); 
  }
  
  public DuccEventHttpDispatcher( String targetEndpoint, int timeout ) throws Exception {
    httpclient = new HttpClient();
    method = new PostMethod(targetEndpoint);
    httpclient.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
    httpclient.getParams().setParameter("http.socket.timeout", timeout);
    httpclient.getParams().setParameter("http.protocol.content-charset", "UTF-8");
  }

    public DuccEvent dispatchAndWaitForDuccReply(DuccEvent duccEvent) 
        throws Exception
    {
        String serBody = XStreamUtils.marshall(duccEvent);
        RequestEntity re = new StringRequestEntity(serBody,"application/xml", "UTF8");
        method.setRequestEntity(re);
        method.setRequestHeader("Content-Type", "text/xml");
        httpclient.executeMethod(method);
        
        //System.out.println("Client Received Reply of type:"+method.getStatusLine().getStatusCode());
        if ( method.getStatusLine().getStatusCode() == 200 ) {

            //        Header[] headers = method.getResponseHeaders();
            //        for ( Header h : headers ) {
            //            System.out.println("Response: " + h.toString());
            //        }
            StringBuffer sb = new StringBuffer();
            byte[] slice = new byte[4096];
            int bytes_read = 0;
            BufferedInputStream bis = new BufferedInputStream(method.getResponseBodyAsStream());
            while ( (bytes_read = bis.read(slice, 0, slice.length)) != -1 ) {
                sb.append(new String(slice, 0, bytes_read));
            }
            return (DuccEvent)XStreamUtils.unmarshall(sb.toString());
        } else {
            String body = method.getResponseBodyAsString();
            throw new DuccRuntimeException("Ducc Unable to Process Request. Http Response Code:"+method.getStatusLine().getStatusCode()+". Ducc Service (OR) Returned Exception:",new Exception(body));
        }
    }

  /**
   * Must call this if done using this class
   */
  public void close() {
    if ( method != null ) {
      method.releaseConnection();
    }
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
