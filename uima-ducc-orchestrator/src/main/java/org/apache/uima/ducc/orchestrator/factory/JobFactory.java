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
package org.apache.uima.ducc.orchestrator.factory;

import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;

public class JobFactory implements IJobFactory {
	
	public static IJobFactory getInstance() {
		IJobFactory jobFactory = null;
		String jd_configuration_class = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_jd_configuration_class);
		if(jd_configuration_class != null) {
			if(jd_configuration_class.trim().equals("org.apache.uima.ducc.jd.config.JobDriverConfiguration")) {
				jobFactory = JobFactoryV1.getInstance();
			}
			else {
				jobFactory = JobFactoryV2.getInstance();
			}
		}
		return jobFactory;
	}

	@Override
	public IDuccWorkJob create(CommonConfiguration common, JobRequestProperties jobRequestProperties) {
		return null;
	}
	
}
