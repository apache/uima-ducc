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
package org.apache.uima.ducc.common.utils.id;

import java.util.UUID;

public class ADuccId
    implements IDuccId
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1025988737223302306L;
	
	private UUID unique;
    private long myFriendly = 0;

    public ADuccId(long given)
    {
        this.unique = UUID.randomUUID();
        myFriendly = given;
    }

    public void setUUID(UUID unique)
    {
    	this.unique = unique;
    }
    
    public int compareTo(Object id)
    { 
        if ( id instanceof ADuccId ) {
            return unique.compareTo(((ADuccId) id).unique);
        } else {
            return this.compareTo(id);
        }
    }

    /* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((unique == null) ? 0 : unique.hashCode());
		return result;
	}
 
    public long getFriendly()
    {
        return myFriendly;
    }

    public void setFriendly(long myFriendly)
    {
        this.myFriendly = myFriendly;
    }
    
    public String toString()
    {
        return ""+myFriendly;
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
		ADuccId other = (ADuccId) obj;
		if (unique == null) {
			if (other.unique != null)
				return false;
		} else if (!unique.equals(other.unique))
			return false;
		return true;
	}
	public String getUnique() {
		return unique.toString();
	}

}
