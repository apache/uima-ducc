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
package org.apache.uima.ducc.orchestrator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.orchestrator.utilities.Checkpointable;
import org.apache.uima.ducc.orchestrator.utilities.TrackSync;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.history.HistoryFactory;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;


public class OrchestratorCheckpoint {
	
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(OrchestratorCheckpoint.class.getName());
	
	private static OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private static Messages messages = orchestratorCommonArea.getSystemMessages();
	
	private static String fileName = orchestratorCommonArea.getStateDirectory()+File.separator+"orchestrator.ckpt";
	
	private static OrchestratorCheckpoint orchestratorCheckpoint = new OrchestratorCheckpoint();

	private static boolean useDb = true;

	public static OrchestratorCheckpoint getInstance() 
    {
		String jhi = System.getProperty("ducc.job.history.impl");
		if(jhi == null) {
			useDb = false;
		}
		else {
			useDb = jhi.contains("database");
		}
		return orchestratorCheckpoint;
	}
	
	public OrchestratorCheckpoint() {
		IOHelper.mkdirs(orchestratorCommonArea.getStateDirectory());
		return;
	}

	private volatile boolean saveEnabled = false;
	private volatile boolean restoreEnabled = false;
	
	private volatile String status = "on";
	
	public boolean switchOnOff(String position) {
		String methodName = "switchOnOff";
		logger.trace(methodName, null, messages.fetch("enter"));
		if(position != null) {
			String desiredPosition = position.toLowerCase();
			if(desiredPosition.equals("off")) {
				resetSaveEnabled();
				resetRestoreEnabled();
				status = desiredPosition;
				logger.debug(methodName, null, messages.fetchLabel("reset to")+position);
			}
			else if(desiredPosition.equals("on")) {
				setSaveEnabled();
				setRestoreEnabled();
				status = desiredPosition;
				logger.debug(methodName, null, messages.fetchLabel("set to")+position);
			}
			else {
				logger.warn(methodName, null, messages.fetchLabel("ignored")+position);
			}
		}
		else {
			setSaveEnabled();
			setRestoreEnabled();
			logger.debug(methodName, null, messages.fetchLabel("missing, using")+status);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return restoreEnabled && saveEnabled;
	}
	
	public boolean isSaveEnabled() {
		String methodName = "isSaveEnabled";
		logger.trace(methodName, null, messages.fetch("enter"));
		logger.debug(methodName, null, saveEnabled);
		logger.trace(methodName, null, messages.fetch("exit"));
		return saveEnabled;
	}
	
	public void setSaveEnabled() {
		String methodName = "setSaveEnabled";
		logger.trace(methodName, null, messages.fetch("enter"));
		saveEnabled = true;
		logger.debug(methodName, null, saveEnabled);
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public void resetSaveEnabled() {
		String methodName = "resetSaveEnabled";
		logger.trace(methodName, null, messages.fetch("enter"));
		saveEnabled = false;
		logger.debug(methodName, null, saveEnabled);
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}

	public boolean isRestoreEnabled() {
		String methodName = "isRestoreEnabled";
		logger.trace(methodName, null, messages.fetch("enter"));
		logger.debug(methodName, null, restoreEnabled);
		logger.trace(methodName, null, messages.fetch("exit"));
		return restoreEnabled;
	}
	
	public void setRestoreEnabled() {
		String methodName = "setRestoreEnabled";
		logger.trace(methodName, null, messages.fetch("enter"));
		restoreEnabled = true;
		logger.debug(methodName, null, restoreEnabled);
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}
	
	public void resetRestoreEnabled() {
		String methodName = "resetRestoreEnabled";
		logger.trace(methodName, null, messages.fetch("enter"));
		restoreEnabled = false;
		logger.debug(methodName, null, restoreEnabled);
		logger.trace(methodName, null, messages.fetch("exit"));
		return;
	}

    private boolean saveStateDb()
    {
		String methodName = "saveStateDb";
        IHistoryPersistenceManager saver = HistoryFactory.getInstance(this.getClass().getName());
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		if(saveEnabled) {
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
			synchronized(workMap) {
				ts.using();
				try
				{
					logger.info(methodName, null, messages.fetchLabel("saving to")+fileName);
					Checkpointable checkpointable = orchestratorCommonArea.getCheckpointable();
                    retVal = saver.checkpoint(checkpointable.getWorkMap(), checkpointable.getProcessToJobMap());
					logger.info(methodName, null, messages.fetchLabel("saved")+fileName);
				}
				catch(Exception e)
				{
					logger.error(methodName, null, e);
				}
			}
			ts.ended();
		}
		else {
			logger.debug(methodName, null, messages.fetchLabel("bypass saving to")+fileName);
		}
        logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
    }

    private boolean saveStateFile()
    {
		String methodName = "saveStateFile";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		if(saveEnabled) {
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
			synchronized(workMap) {
				ts.using();
				try
				{
					logger.info(methodName, null, messages.fetchLabel("saving to")+fileName);
					FileOutputStream fos = null;
					ObjectOutputStream out = null;
					fos = new FileOutputStream(fileName);
					out = new ObjectOutputStream(fos);
					Checkpointable checkpointable = orchestratorCommonArea.getCheckpointable();
					out.writeObject(checkpointable);
					out.close();
					retVal = true;
					logger.info(methodName, null, messages.fetchLabel("saved")+fileName);
				}
				catch(IOException e)
				{
					logger.error(methodName, null, e);
				}
			}
			ts.ended();
		}
		else {
			logger.debug(methodName, null, messages.fetchLabel("bypass saving to")+fileName);
		}
        logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
    }

	public boolean saveState() 
    {
        // we can resolve these into just one call by allowing the checkpointable to be saved in its parts for
        // the file implementation, to avoid circular dependencies
        if ( useDb ) return saveStateDb();
        else         return saveStateFile();
	}

    private boolean restoreStateDb()
    {
		String methodName = "restoreState";
		logger.trace(methodName, null, messages.fetch("enter"));
        IHistoryPersistenceManager saver = HistoryFactory.getInstance(this.getClass().getName());
		boolean retVal = false;
		if(saveEnabled) {
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
			synchronized(workMap) {
				ts.using();
				try
				{
					logger.info(methodName, null, messages.fetchLabel("restoring from")+fileName);
                    DuccWorkMap work = new DuccWorkMap();
                    ConcurrentHashMap<DuccId, DuccId> processToJob = new ConcurrentHashMap<DuccId, DuccId>();
                    Checkpointable checkpointable = new Checkpointable(work, processToJob);
                    retVal = saver.restore(work, processToJob);
					orchestratorCommonArea.setCheckpointable(checkpointable);
					logger.info(methodName, null, messages.fetch("restored"));
				}
				catch(ClassNotFoundException e)
				{
					logger.error(methodName, null, e);
				}

				catch(Exception e)
				{
					logger.warn(methodName, null, e);
				}
			}
			ts.ended();
		}
		else {
			logger.info(methodName, null, messages.fetchLabel("bypass restoring from")+fileName);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
    }

	private boolean restoreStateFile() {
		String methodName = "restoreState";
		logger.trace(methodName, null, messages.fetch("enter"));
		boolean retVal = false;
		if(saveEnabled) {
			DuccWorkMap workMap = orchestratorCommonArea.getWorkMap();
			TrackSync ts = TrackSync.await(workMap, this.getClass(), methodName);
			synchronized(workMap) {
				ts.using();
				try
				{
					logger.info(methodName, null, messages.fetchLabel("restoring from")+fileName);
					FileInputStream fis = null;
					ObjectInputStream in = null;
					fis = new FileInputStream(fileName);
					in = new ObjectInputStream(fis);
					Checkpointable checkpointable = (Checkpointable)in.readObject();
                    DuccWorkMap map = checkpointable.getWorkMap();
    
                    Set<DuccId> ids = map.getReservationKeySet();
                    for ( DuccId id : ids ) {
                        DuccWorkReservation r = (DuccWorkReservation) map.findDuccWork(DuccType.Reservation, ""+id.getFriendly());
                        logger.info(methodName, id, "Looking for work: r", r);
                        if ( r != null ) r.initLogger();
                    }
                
					orchestratorCommonArea.setCheckpointable(checkpointable);
					in.close();
					retVal = true;
					logger.info(methodName, null, messages.fetch("restored"));
				}
				catch(IOException e)
				{
					logger.warn(methodName, null, e);
				}
				catch(ClassNotFoundException e)
				{
					logger.error(methodName, null, e);
				}
			}
			ts.ended();
		}
		else {
			logger.info(methodName, null, messages.fetchLabel("bypass restoring from")+fileName);
		}
		logger.trace(methodName, null, messages.fetch("exit"));
		return retVal;
	}
	
    public boolean restoreState()
    {
        // we can resolve these into just one call by allowing the checkpointable to be saved in its parts for
        // the file implementation, to avoid circular dependencies

        if ( useDb ) return restoreStateDb();
        else         return restoreStateFile();
    }

}
