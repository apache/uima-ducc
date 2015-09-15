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
