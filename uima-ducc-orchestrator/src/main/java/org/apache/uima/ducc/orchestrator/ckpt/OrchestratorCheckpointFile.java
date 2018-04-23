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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Set;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.utilities.Checkpointable;
import org.apache.uima.ducc.orchestrator.utilities.TrackSync;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;

public class OrchestratorCheckpointFile extends AOrchestratorCheckpoint implements IOrchestratorCheckpoint {
	
	private static DuccLogger logger = DuccLogger.getLogger(OrchestratorCheckpointFile.class);
	private static DuccId jobid = null;

	private static String saveLocation = orchestratorCommonArea.getStateDirectory()+File.separator+"orchestrator.ckpt";
	
	protected OrchestratorCheckpointFile() {
		IOHelper.mkdirs(orchestratorCommonArea.getStateDirectory());
		ckptOnOff();
	}
	
	@Override
	public boolean saveState() {
		String location = "saveState";
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
					FileOutputStream fos = null;
					ObjectOutputStream out = null;
					fos = new FileOutputStream(saveLocation);
					out = new ObjectOutputStream(fos);
					Checkpointable checkpointable = orchestratorCommonArea.getCheckpointable();
					out.writeObject(checkpointable);
					out.close();
					retVal = true;
					logger.info(location, jobid, messages.fetchLabel("saved ")+saveLocation);
				}
				catch(IOException e)
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
		boolean retVal = false;
		if(isCkptEnabled()) {
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			TrackSync ts = TrackSync.await(workMap, this.getClass(), location);
			synchronized(workMap) {
				ts.using();
				try
				{
					logger.info(location, jobid, messages.fetchLabel("restoring from ")+saveLocation);
					FileInputStream fis = null;
					ObjectInputStream in = null;
					fis = new FileInputStream(saveLocation);
					in = new ObjectInputStream(fis);
					Checkpointable checkpointable = (Checkpointable)in.readObject();
                    DuccWorkMap map = checkpointable.getWorkMap();
    
                    Set<DuccId> ids = map.getReservationKeySet();
                    for ( DuccId id : ids ) {
                        DuccWorkReservation r = (DuccWorkReservation) map.findDuccWork(DuccType.Reservation, ""+id.getFriendly());
                        logger.info(location, id, "Looking for work: r", r);
                        if ( r != null ) r.initLogger();
                    }
                
					orchestratorCommonArea.setCheckpointable(checkpointable);
					in.close();
					retVal = true;
					logger.info(location, jobid, messages.fetch("restored"));
				}
				catch(IOException e)
				{
					logger.warn(location, jobid, e);
				}
				catch(ClassNotFoundException e)
				{
					logger.error(location, jobid, e);
				}
			}
			ts.ended();
		}
		else {
			logger.info(location, jobid, messages.fetchLabel("bypass restoring from ")+saveLocation);
		}
		logger.trace(location, jobid, messages.fetch("exit"));
		return retVal;
	}

}
