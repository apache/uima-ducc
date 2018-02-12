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


import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.uima.ducc.common.utils.DuccProperties;
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
//            "lib/apache-camel/xstream*",
        "apache-uima/apache-activemq/lib/optional/xstream*",
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

    private void secureXStream(Object xStream_obj) throws Exception {
        Class<?> c = classManager.loadClass("com.thoughtworks.xstream.XStream");
        Method m = c.getDeclaredMethod("setupDefaultSecurity", new Class[] {c});
        m.invoke(null, new Object[] {xStream_obj });
        Object noTypePermissionObject = classManager.construct("com.thoughtworks.xstream.security.NoTypePermission");
        Field noneField = noTypePermissionObject.getClass().getDeclaredField("NONE");
        Object anyTypePermissionObject = classManager.construct("com.thoughtworks.xstream.security.AnyTypePermission");
        Field anyField = anyTypePermissionObject.getClass().getDeclaredField("ANY");
        
        classManager.invoke(xStream_obj, "addPermission", new Object[] {noneField.get(null)});
        classManager.invoke(xStream_obj, "addPermission", new Object[] {anyField.get(null)});
   }
    String toXml(Object ev)
        throws Exception
    {        
        Object dd_obj = classManager.construct("com.thoughtworks.xstream.io.xml.DomDriver", new Object[] {null});

        Object   xStream_obj = classManager.construct("com.thoughtworks.xstream.XStream", new Object[] {dd_obj});

        secureXStream(xStream_obj);
        String serializaedMsg =  (String) classManager.invoke(xStream_obj, "toXML", new Object[] {ev});
        return serializaedMsg;
    
    }

    Object fromXml(String str)
        throws Exception
    {        
        Object   dd_obj = classManager.construct("com.thoughtworks.xstream.io.xml.DomDriver", new Object[] {null});

        Object   xStream_obj = classManager.construct("com.thoughtworks.xstream.XStream", new Object[] {dd_obj});
        secureXStream(xStream_obj);
       return classManager.invoke(xStream_obj, "fromXML", new Object[] {str});        
    }

    Object fromJson(String str, Class<?> cl)
        throws Exception
    {        
        Object   gson_obj = classManager.construct("com.google.gson.Gson");

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
        	System.setProperty("DUCC_HOME","/users/cwiklik/releases/builds/uima-ducc/2.2.2/target/apache-uima-ducc-2.2.2-SNAPSHOT");
         	String[] classpath = {
//                  "lib/apache-camel/xstream*",
              "apache-uima/apache-activemq/lib/optional/xstream*",
              "lib/google-gson/gson*",
          };      
        	ClassManager classManager = new ClassManager(classpath);
            Class nullPermissionClaz = classManager.loadClass("com.thoughtworks.xstream.security.NullPermission");
            Class primitiveTypePermissionClaz = classManager.loadClass("com.thoughtworks.xstream.security.PrimitiveTypePermission");
            Object dd_obj = classManager.construct("com.thoughtworks.xstream.io.xml.DomDriver", new Object[] {null});
            
            Object noTypePermissionObject = classManager.construct("com.thoughtworks.xstream.security.NoTypePermission");
            Field noneField = noTypePermissionObject.getClass().getDeclaredField("NONE");
            
            Object nullPermissionObject = classManager.construct("com.thoughtworks.xstream.security.NullPermission");
            Field nullField = nullPermissionObject.getClass().getDeclaredField("NULL");

            Object primitiveTypePermissionObject = classManager.construct("com.thoughtworks.xstream.security.PrimitiveTypePermission");
            Field primitivesField = primitiveTypePermissionObject.getClass().getDeclaredField("PRIMITIVES");

            
            Object   xStream_obj = classManager.construct("com.thoughtworks.xstream.XStream", new Object[] {dd_obj});
            
            
            Class c = classManager.loadClass("com.thoughtworks.xstream.XStream");
            Method m = c.getDeclaredMethod("setupDefaultSecurity", new Class[] {c});
            m.invoke(null, new Object[] {xStream_obj });

            classManager.invoke(xStream_obj, "addPermission", new Object[] {noneField.get(null)});
            classManager.invoke(xStream_obj, "addPermission", new Object[] {nullField.get(null)});
            classManager.invoke(xStream_obj, "addPermission", new Object[] {primitivesField.get(null)});
            
//            classManager.invoke(xStream_obj, "allowTypeHierarchy", new Object[] {Collection.class});
            classManager.invoke(xStream_obj, "allowTypesByWildcard", new Object[] {new String[] {"org.apache.uima.*"}});

            Map<String,String> map = new HashMap<>();
            String s = " Tests";
            map.put("this", s);
            org.apache.uima.ducc.transport.event.SubmitJobDuccEvent event1 = 
            		new org.apache.uima.ducc.transport.event.SubmitJobDuccEvent(new DuccProperties(), 1);
            String serializaedMsg =  (String) classManager.invoke(xStream_obj, "toXML", new Object[] {event1});
            
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
