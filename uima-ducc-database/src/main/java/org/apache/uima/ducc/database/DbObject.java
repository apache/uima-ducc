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
package org.apache.uima.ducc.database;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.database.DbConstants.DbVertex;


/**
 * Simple holder class to return stuff put into the database.
 */
public class DbObject
{
    String json;
    DbVertex type;
    List<DbObject> embedded;
    
    DbObject(String json, DbVertex type)
    {
        embedded = new ArrayList<DbObject>();
        this.json = json;
        this.type = type;
    }

    void addEmbedded(DbObject obj)
    {
        embedded.add(obj);
    }

    public String getJson()             { return json;     }
    public DbVertex getType()            { return type;     }
    public List<DbObject> getEmbedded() { return embedded; }    
}
