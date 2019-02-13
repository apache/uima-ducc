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
package org.apache.uima.ducc.ps.service.transport;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;
import com.thoughtworks.xstream.security.AnyTypePermission;
import com.thoughtworks.xstream.security.NoTypePermission;

public class XStreamUtils {
	
	private static void initXStreanSecurity(XStream xStream) {
		XStream.setupDefaultSecurity(xStream);
		xStream.addPermission(NoTypePermission.NONE);
		xStream.addPermission(AnyTypePermission.ANY);
	}
	public static String marshall( Object targetToMarshall) throws Exception {
        synchronized(XStreamUtils.class) {
    		XStream xStream = new XStream(new DomDriver());
    		initXStreanSecurity(xStream);
            return xStream.toXML(targetToMarshall); 
        }
	}
	public static Object unmarshall( String targetToUnmarshall) throws Exception {
        synchronized(XStreamUtils.class) {
    		XStream xStream = new XStream(new DomDriver());
    		initXStreanSecurity(xStream);
    		//System.out.println("Recv'd:"+targetToUnmarshall);
    		return xStream.fromXML(targetToUnmarshall);
        }
	}
}
