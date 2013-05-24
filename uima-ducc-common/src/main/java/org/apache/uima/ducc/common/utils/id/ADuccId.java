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

/**
 * This class provudes a unique identifier to various DUCC objects.  It is used to uniquely identify
 * DUCC Jobs, Reservations, Registered Services, Service Instances, Arbitrary Processes, resource
 * shares, and so on.
 *
 * A DuccId consists of two fields, A UUID which is usually hidden, and which is used internally,
 * and a "friendly", numeric id, intended for better consumption by human beings.
 *
 * The DuccId implements its own compareTo(), hashCode() and equals() methods and should be used as
 * a primitive object by all internal DUCC components.  When exposing a DuccId to the world, use the
 * "friendly" id.
 */

public class ADuccId
    implements IDuccId
{
	private static final long serialVersionUID = 1025988737223302306L;
	
	private UUID unique;
    private long myFriendly = 0;

    /**
     * Constructor - create a UNIQUE id, presenting a specific "frienly" id as needed.  The
     * DuccId it produces is unique for all intents and purposes, even if the "friendly" isn't.
     *
     * @param given This is the "friendly" id which may not be unique.
     */
    public ADuccId(long given)
    {
        this.unique = UUID.randomUUID();
        myFriendly = given;
    }

    /**
     * Create a DuccId from a given UUID. This is used internally to restore a DuccId from some
     * serialized resource.
     *
     * @param unique This is a unique ID which overrides the generated UUID from the constructor.  
     *        Use with care.
     */
    public void setUUID(UUID unique)
    {
    	this.unique = unique;
    }
    
    /* (non-Javadoc)
	 * @see java.lang.compareTo()
	 */
	@Override
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
 
    /**
     * Return the (not unique) "friendly" id.
     * @return THe non-unique friendly id.
     */
    public long getFriendly()
    {
        return myFriendly;
    }

    /**
     * Set a friendly id.  
     * @param myFriendly The "friendly" id to associate with the UUID.
     */
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
