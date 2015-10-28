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

package org.apache.uima.ducc.common.persistence;

/**
 * All properties sent to the DB must implement this interface.
 */
public interface IDbProperty
{
    String  pname();                 // This is the name of the property.  Any java.util.Property key .toString() is valid.
    Type    type();                  // One of the types below.  DB impl translates to db equialent.  Try to keep these
                                     //     an easy analogy of java names.  DB does not convert so the type of the incoming
                                     //     propety has to match.
    boolean isPrimaryKey();          // Primary key in DB.  Many columns can have this which results in a compount key. Order
                                     //     is important - first is cluster key, and following are the others.
    boolean isPrivate();             // Used only in the DB, do not pass back to the application in Properties.  This goes into the db and
                                     //     is part of the schema.
    boolean isMeta();                // A handy constant in the enum that isn't part of a schema.  Helps in the generation
                                     //     of the schema.  Doesn't go into the DB.
    String  columnName();            // Because we want to persist java.util.Properties, and these things
                                     //     allow a greater range of characters in the keys ('.', '-' and others)
                                     //     than does SQL, we define a translation from the "properties" key to
                                     //     legal SQL syntactic names.  DB does not translate, user must provide
                                     //     a suitable translation.

    // If we update this we may have to update db methods that use it
    public enum Type {
        String,            // Java String
            Blob,          // Java serialized object or other binary
            Boolean,       // Java boolean
            Integer,
            Long,
            Double,
            UUID,
    };
}
