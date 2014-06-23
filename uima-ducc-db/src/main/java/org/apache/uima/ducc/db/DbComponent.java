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
package org.apache.uima.ducc.db;

import java.util.Properties;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.db.portal.DbPortal;
import org.apache.uima.ducc.transport.event.DbComponentStateEvent;
import org.apache.uima.ducc.transport.event.IDbComponentProperties;
import org.apache.uima.ducc.transport.event.IDbComponentProperties.ConfigurationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({CommonConfiguration.class})
public class DbComponent extends AbstractDuccComponent 
implements IDbComponent {
	
	//	Springframework magic to inject instance of {@link CommonConfiguration}
	@Autowired CommonConfiguration common;
	
	private static final DuccLogger logger = DuccLoggerComponents.getDbLogger(DbComponent.class.getName());
	
	private static DuccId jobid = null;
	
	private DbPortal dbPortal = null;
	
	private String description = "Database Services";
	
	public DbComponent(CamelContext context) {
		super("DB Daemon", context);
		initialize();
	}

    public DuccLogger getLogger()
    {
        return logger;
    }

	public DbPortal getDbPortal() {
		return dbPortal;
	}
	
	private void initialize() {
		String location = "initialize";
		try {
			dbPortal = new DbPortal();
			switch(dbPortal.getDbMode()) {
			case Enabled:
				DbComponentCommonArea.getInstance().setDbComponentProperty(IDbComponentProperties.keyConfigurationStatus, ConfigurationStatus.Enabled.name());
				break;
			case Disabled:
				DbComponentCommonArea.getInstance().setDbComponentProperty(IDbComponentProperties.keyConfigurationStatus, ConfigurationStatus.Disabled.name());
				break;
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
			int code = -1;
			System.exit(code);
		}
	}
	
	/*
	@Override
	public void start(DuccService service) throws Exception {
		String location = "start";
		super.start(service);
		logger.info(location, jobid, description);
		DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.DbManager,getProcessJmxUrl());
	}
	*/
	
	@Override
	public void start(DuccService service, String[] args) throws Exception {
		String location = "start";
		super.start(service, args);
		logger.info(location, jobid, description);
		DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.DbManager,getProcessJmxUrl());
	}
	
	@Override
	public void stop() throws Exception {
		String location = "stop";
		logger.info(location, jobid, description);
		super.stop();
	}
	
	@Override
	public DbComponentStateEvent getState() {
		String location = "getState";
		DbComponentStateEvent retVal = new DbComponentStateEvent();
		try {
			Properties properties = DbComponentCommonArea.getInstance().getDbComponentPropertiesCopy();
			retVal.setProperties(properties);
			logger.info(location, jobid, properties.size());
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return retVal;
	}

}
