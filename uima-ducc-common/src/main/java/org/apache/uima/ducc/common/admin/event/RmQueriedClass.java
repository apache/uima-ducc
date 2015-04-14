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
package org.apache.uima.ducc.common.admin.event;

import java.io.Serializable;

public class RmQueriedClass
	implements Serializable
{
	private static final long serialVersionUID = -8101741014979144426L;

    private String name;
    private String policy;
    private int[] demanded;
    private int[] awarded;

    public RmQueriedClass()
    {
    }

	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}

    public String getPolicy() {
        return policy;
    }

    public void setPolicy(String policy) {
        this.policy = policy;
    }

	public int[] getDemanded() {
		return demanded;
	}


	public void setDemanded(int[] demanded) {
		this.demanded = demanded;
	}


	public int[] getAwarded() {
		return awarded;
	}


	public void setAwarded(int[] awarded) {
		this.awarded = awarded;
	}

    public String toCompact()
    {
        return "";
    }

    public String toConsole()
    {
        StringBuffer sb = new StringBuffer();
        sb.append("Class ");
        sb.append(name);
        sb.append(" ");
        sb.append(policy);
        sb.append("\n");
        sb.append("   Requested: ");
        sb.append(RmAdminQLoadReply.fmtArray(demanded));
        sb.append("\n   Awarded  : ");
        sb.append(RmAdminQLoadReply.fmtArray(awarded));
        sb.append("\n");
        return sb.toString();
    }
    
}
