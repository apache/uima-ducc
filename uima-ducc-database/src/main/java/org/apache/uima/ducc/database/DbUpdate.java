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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

/**
 * The purpose of this class is to update the DB with additions for newer versions of DUCC.
 * 
 * For example, when upgrading from DUCC 2.2.2 to 3.0.0 the new column "quiesced" needs to
 * be added to the rmnodes table.  The methodology is to call the RmStatePersistence class to
 * get an alter statement, and execute it against the "old" database in order to modernize it.
 * If installing 3.0.0, the column is already there.  Adding the column more than once is harmless.
 * 
 * This code was originally designed to be called by start_ducc whenever the database is started.
 */
public class DbUpdate
{
	private DuccLogger logger = null;
	
	public static final String DUCC_KEYSPACE = "ducc";
	public static final String DUCC_DATABASE_USER = "ducc";
	
	private String ducc_home = System.getProperty("DUCC_HOME");
	private String ducc_properties = Paths.get(ducc_home,"resources","ducc.properties").toString();
	private String ducc_private_properties = Paths.get(ducc_home,"resources.private","ducc.private.properties").toString();
	
	public DbUpdate() {
		if (ducc_home == null) {
            throw new IllegalStateException("DUCC_HOME must be set as a system property: -DDUCC_HOME=whatever");
        }
	}
	
	public Properties getDuccProperties(String filePath) {
		Properties properties = null;
		try {
			File file = new File(filePath);
			FileInputStream fis = new FileInputStream(file);
			properties = new Properties();
			properties.load(fis);
			fis.close();
		}
		catch (FileNotFoundException e) {
			throw new IllegalStateException(e);
		} 
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
		return properties;
	}
	
	private String getDbUser(Properties properties) {
		String retVal = null;
		String key = "ducc.database.user";
		retVal = properties.getProperty(key);
		if(retVal == null) {
			retVal = DUCC_DATABASE_USER;
		}
		return retVal;
	}
	
	private String getDbHost(Properties properties) {
		String retVal = null;
		String key = "ducc.database.host";
		retVal = properties.getProperty(key);
		return retVal;
	}
	
	private String getDbHostList(Properties properties) {
		String retVal = null;
		String key = "ducc.database.host.list";
		retVal = properties.getProperty(key);
		if(retVal == null) {
			retVal = getDbHost(properties);
		}
		return retVal;
	}
	
	private String getDbPw(Properties properties) {
		String retVal = null;
		String key = "db_password";
		retVal = properties.getProperty(key);
		return retVal;
	}
	
	private void updateDb(Session session) {
		updateDb(session, RmStatePersistence.getAlterList());
	}
	
	private void updateDb(Session session, List<SimpleStatement> list) {
		String location = "updateDb";
		for ( SimpleStatement s : list ) {
            doLog(location, "EXECUTE STATEMENT:", s.toString());
            try {
            	session.execute(s);
            }
            catch(InvalidQueryException e) {
            	String cause = e.getMessage();
            	if(cause.contains("conflicts with an existing column")) {
            		//OK
            	}
            	else {
            		throw e;
            	}
            }
        }
	}

	private void doLog(String methodName, Object ... msg) {      
		if ( logger == null ) {
			StringBuffer buf = new StringBuffer(methodName);
			for ( Object o : msg ) {
				buf.append(" ");
				if ( o == null ) {
					buf.append("<null>");
				} 
				else {
					buf.append(o.toString());
				} 
			}            
			System.out.println(buf);
		} 
		else {
			logger.info(methodName, null, msg);
			return;
		}
	}
	
	public void update() {
		Properties propsPublic = getDuccProperties(ducc_properties);
		Properties propsPrivate = getDuccProperties(ducc_private_properties);
		String ducc_db_user = getDbUser(propsPublic);
		String ducc_db_host_list = getDbHostList(propsPublic);
		String[] ducc_db_host_array = (String[])(Arrays.asList(ducc_db_host_list.split("\\s+")).toArray());
		String ducc_db_pw = getDbPw(propsPrivate);
		//System.out.println(ducc_db_host_list);
		//System.out.println(ducc_db_user);
		//System.out.println(ducc_db_pw);
		AuthProvider auth = new PlainTextAuthProvider(ducc_db_user, ducc_db_pw);
		Cluster cluster = Cluster.builder()
                .withAuthProvider(auth)
                .addContactPoints(ducc_db_host_array)
                .build();
		Session session = cluster.connect();
		session.execute("USE " + DUCC_KEYSPACE);
		updateDb(session);
		session.close();
		cluster.close();
	}
	
    public static void main(String[] args)
    {
        int rc = 0;
    	try {
        	DbUpdate dbAlter = new DbUpdate();
        	dbAlter.update();
        } 
        catch ( Throwable e ) {
            System.out.println("Errors altering database");
            e.printStackTrace();
            rc = 1;
        } 
        System.exit(rc);
    }

}
