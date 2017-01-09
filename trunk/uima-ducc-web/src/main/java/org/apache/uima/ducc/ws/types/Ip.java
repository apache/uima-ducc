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
package org.apache.uima.ducc.ws.types;

public class Ip implements Comparable<Ip> {
	
	private String ip;
	
	public Ip(String ip) {
		this.ip = ip;
	}
	
	@Override
	public String toString() {
		return this.ip;
	}
	
	@Override
	public int compareTo(Ip ip) {
		int retVal = 0;
		if(ip != null) {
			Ip that = ip;
			String thatIp = that.toString();
			String thisIp = this.toString();
			retVal = thisIp.compareTo(thatIp);
		}
		return retVal;
	}
	
	@Override
	public boolean equals(Object object) {
		boolean retVal = false;
		if(object != null) {
			if(object instanceof Ip) {
				Ip that = (Ip) object;
				String thatIp = that.toString();
				String thisIp = this.toString();
				retVal = thisIp.equals(thatIp);
			}
		}
		return retVal;
	}
	
	// @return use ip as hashCode
	
	@Override
	public int hashCode()
	{
		return ip.hashCode();
	}
}
