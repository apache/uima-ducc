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
package org.apache.uima.ducc.common;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NodeIdentity implements Serializable {
	private static final long serialVersionUID = -3725003472985192870L;
	private String name;
	private String ip;
	private List<IIdentity> nodeIdentities = new ArrayList<IIdentity>();
	public NodeIdentity(String ip, String name) throws Exception {
		setName(name);
		setIp(ip);
		nodeIdentities.add( new Identity(name, ip));
	}
	public NodeIdentity() throws Exception {
		setName(InetAddress.getLocalHost().getCanonicalHostName());
		setIp(InetAddress.getLocalHost().getHostAddress());
		
		Enumeration<NetworkInterface> netinterfaces = 
				NetworkInterface.getNetworkInterfaces();
	    while (netinterfaces.hasMoreElements()) {
	        NetworkInterface iface = netinterfaces.nextElement();
	        Enumeration<InetAddress> addresses = iface.getInetAddresses();
	        while (addresses.hasMoreElements()) {
	            InetAddress add = addresses.nextElement();
	    		nodeIdentities.add( new Identity(add.getCanonicalHostName(), add.getHostAddress()));
	        }
	    }
	}
	public List<IIdentity> getNodeIdentities() {
		return nodeIdentities;
	}

	public String getName() {
		return name;
	}

	protected void setName(String name) {
		this.name = name;
	}

	public String getIp() {
		return ip;
	}

	protected void setIp(String ip) {
		this.ip = ip;
	}

	@Override
	public String toString() {
		return "Name:"+name+" IP:"+ip;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeIdentity other = (NodeIdentity) obj;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
}
