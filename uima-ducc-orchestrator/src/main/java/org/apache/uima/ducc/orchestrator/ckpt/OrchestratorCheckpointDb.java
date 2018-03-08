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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.utilities.Checkpointable;
import org.apache.uima.ducc.orchestrator.utilities.TrackSync;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.history.HistoryFactory;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;

public class OrchestratorCheckpointDb extends AOrchestratorCheckpoint implements IOrchestratorCheckpoint {
	
	private static DuccLogger logger = DuccLogger.getLogger(OrchestratorCheckpointDb.class);
	private static DuccId jobid = null;

	private static String saveLocation = "db";

	protected OrchestratorCheckpointDb() {
		IOHelper.mkdirs(orchestratorCommonArea.getStateDirectory());
		ckptOnOff();
	}

	@Override
	public boolean saveState() {
		String location = "saveState";
        IHistoryPersistenceManager saver = HistoryFactory.getInstance(this.getClass().getName());
		logger.trace(location, jobid, messages.fetch("enter"));
		boolean retVal = false;
		if(isCkptEnabled()) {
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			TrackSync ts = TrackSync.await(workMap, this.getClass(), location);
			synchronized(workMap) {
				ts.using();
				try
				{
					logger.info(location, jobid, messages.fetchLabel("saving to ")+saveLocation);
					Checkpointable checkpointable = orchestratorCommonArea.getCheckpointable();
                    retVal = saver.checkpoint(checkpointable.getWorkMap(), checkpointable.getProcessToJobMap());
					logger.info(location, jobid, messages.fetchLabel("saved ")+saveLocation);
				}
				catch(Exception e)
				{
					logger.error(location, jobid, e);
				}
			}
			ts.ended();
		}
		else {
			logger.debug(location, jobid, messages.fetchLabel("bypass saving to ")+saveLocation);
		}
        logger.trace(location, jobid, messages.fetch("exit"));
		return retVal;
	}

	@Override
	public boolean restoreState() {
		String location = "restoreState";
		logger.trace(location, jobid, messages.fetch("enter"));
        IHistoryPersistenceManager saver = HistoryFactory.getInstance(this.getClass().getName());
		boolean retVal = false;
		if(isCkptEnabled()) {
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			TrackSync ts = TrackSync.await(workMap, this.getClass(), location);
			synchronized(workMap) {
				ts.using();
				try
				{
					logger.info(location, jobid, messages.fetchLabel("restoring from ")+saveLocation);
                    Pair<DuccWorkMap, Map<DuccId, DuccId>> ret = saver.restore();
                    if ( ret.first() != null ) {
                        Checkpointable checkpointable = new Checkpointable(ret.first(), (ConcurrentHashMap<DuccId, DuccId>) ret.second());
                        orchestratorCommonArea.setCheckpointable(checkpointable);
                        logger.info(location, null, messages.fetch("restored"));
                    } else {
                        logger.info(location, null, "No checkpoint found.");
                    }
				}
				catch(ClassNotFoundException e)
				{
					logger.error(location, null, e);
				}

				catch(Exception e)
				{
					logger.warn(location, null, e);
				}
			}
			ts.ended();
		}
		else {
			logger.info(location, null, messages.fetchLabel("bypass restoring from ")+saveLocation);
		}
		logger.trace(location, null, messages.fetch("exit"));
		return retVal;
	}

}
