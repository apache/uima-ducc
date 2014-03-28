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
package org.apache.uima.ducc.sm;

import java.io.Serializable;

import org.apache.uima.ducc.common.IServiceStatistics;

/**
 * Internal (to SM) object for returning ping responses.
 */
public class Pong
    implements Serializable
{
	private static final long serialVersionUID = 1L;

	IServiceStatistics statistics;
    int additions;
    Long[] deletions;
    boolean excessiveFailures = false;
    boolean autostart = true;
    long last_use = 0l;

    public Pong()
    {
    }

    public IServiceStatistics getStatistics() 
    {
		return statistics;
	}

    public void setStatistics(IServiceStatistics statistics) 
    {
		this.statistics = statistics;
	}

	public int getAdditions() 
    {
		return additions;
	}
    
	public void setAdditions(int additions) 
    {
		this.additions = additions;
	}
    
	public Long[] getDeletions() 
    {
		return deletions;
	}
    
	public void setDeletions(Long[] deletions) 
    {
		this.deletions = deletions;
	}

    public void setExcessiveFailures(boolean er)
    {
        this.excessiveFailures = er;
    }
    
    public boolean isExcessiveFailures()
    {
    	return this.excessiveFailures;
    }

    public void setAutostart(boolean a)
    {
        this.autostart = a;
    }

    public boolean isAutostart()
    {
        return autostart;
    }

    public void setLastUse(Long lu)
    {
        this.last_use = lu;
    }

    public long getLastUse()
    {
        return this.last_use;
    }
}
