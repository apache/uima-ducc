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
package org.apache.uima.ducc.db.management;

import org.apache.uima.ducc.db.management.exception.MissingDbDriverException;
import org.apache.uima.ducc.db.management.exception.MissingDbHomeException;
import org.apache.uima.ducc.db.management.exception.MissingDbNameException;
import org.apache.uima.ducc.db.management.exception.MissingDbPasswordException;
import org.apache.uima.ducc.db.management.exception.MissingDbProtocolException;
import org.apache.uima.ducc.db.management.exception.MissingDbUserException;

public class DbContext {
	
	private String dbHome = null;
	private String dbName = null;
	private String dbUser = null;
	private String dbPassword = null;
	private String dbDriver = null;
	private String dbProtocol = null;
	
	private DbMode dbMode = DbMode.Disabled;
	
	public enum DbMode { Enabled, RecordOnly, Disabled };
	
	public DbContext() throws MissingDbHomeException, MissingDbNameException, MissingDbUserException, MissingDbPasswordException, MissingDbDriverException, MissingDbProtocolException {
		setDbHome(DbProperties.getDbHome());
		setDbName(DbProperties.getDbName());
		setDbUser(DbProperties.getDbUser());
		setDbPassword(DbProperties.getDbPassword());
		setDbDriver(DbProperties.getDbDriver());
		setDbProtocol(DbProperties.getDbProtocol());
		switch(dbMode) {
		case Enabled:
			if(dbHome == null) {
				throw new MissingDbHomeException();
			}
			if(dbName == null) {
				throw new MissingDbNameException();
			}
			if(dbUser == null) {
				throw new MissingDbUserException();
			}
			if(dbPassword == null) {
				throw new MissingDbPasswordException();
			}
			if(dbDriver == null) {
				throw new MissingDbDriverException();
			}
			if(dbProtocol == null) {
				throw new MissingDbProtocolException();
			}
			break;
		case Disabled:
			break;
		}
	}

	//
	
	public String getDbHome() {
		return dbHome;
	}
	
	private void setDbHome(String value) {
		dbHome = value;
		if(value != null) {
			setDbMode(DbMode.Enabled);
		}
	}
	
	//
	
	public String getDbName() {
		return dbName;
	}
	
	private void setDbName(String value) {
		dbName = value;
		if(value != null) {
			setDbMode(DbMode.Enabled);
		}
	}
	
	//
	
	public String getDbUser() {
		return dbUser;
	}
	
	private void setDbUser(String value) {
		dbUser = value;
		if(value != null) {
			setDbMode(DbMode.Enabled);
		}
	}
	
	//
	
	public String getDbPassword() {
		return dbPassword;
	}
	
	private void setDbPassword(String value) {
		dbPassword = value;
		if(value != null) {
			setDbMode(DbMode.Enabled);
		}
	}
	
	//
	
	public String getDbDriver() {
		return dbDriver;
	}
	
	private void setDbDriver(String value) {
		dbDriver = value;
		if(value != null) {
			setDbMode(DbMode.Enabled);
		}
	}
	
	//
	
	public String getDbProtocol() {
		return dbProtocol;
	}
	
	private void setDbProtocol(String value) {
		dbProtocol = value;
		if(value != null) {
			setDbMode(DbMode.Enabled);
		}
	}
	
	//
	
	public DbMode getDbMode() {
		return dbMode;
	}
	
	private void setDbMode(DbMode value) {
		dbMode = value;
	}
	
	
}
