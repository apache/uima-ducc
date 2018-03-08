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
package org.apache.uima.ducc.orchestrator.ckpt;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;


public class OrchestratorCheckpoint  {
	
	private static final DuccLogger logger = DuccLogger.getLogger(OrchestratorCheckpoint.class);
	private static final DuccId jobid = null;

	private static IOrchestratorCheckpoint instance = null;
	
	public static IOrchestratorCheckpoint getInstance() 
    {
		String location = "getInstance";
		if(instance == null) {
			boolean useDb = false;
			String jhi = System.getProperty("ducc.job.history.impl");
			if(jhi != null) {
				useDb = jhi.contains("database");
			}
			if(useDb) {
				instance = new OrchestratorCheckpointDb();
				logger.debug(location, jobid, "checkpoint to DB");
			}
			else {
				instance = new OrchestratorCheckpointFile();
				logger.debug(location, jobid, "checkpoint to file");
			}
		}
		return instance;
    }
		
}
