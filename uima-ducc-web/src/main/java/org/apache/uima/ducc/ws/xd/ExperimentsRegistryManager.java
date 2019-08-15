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
package org.apache.uima.ducc.ws.xd;

import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.ws.log.WsLog;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

public class ExperimentsRegistryManager {

  // NOTE - this variable used to hold the class name before WsLog was simplified
	private static DuccLogger cName = DuccLogger.getLogger(ExperimentsRegistryManager.class);
	
	private static Gson gson = new Gson();
	private static ExperimentsRegistryManager instance = new ExperimentsRegistryManager();
	
	private int MAX_CACHE_SIZE = 4096;
	
	private TreeMap<String,IExperiment> map = new TreeMap<String,IExperiment>();
	
	private AtomicLong updateCounter = new AtomicLong(0);
	
	public static ExperimentsRegistryManager getInstance() {
		return instance;
	}
	
	public TreeMap<IExperiment,String> getMapByStatus() {
		TreeMap<IExperiment,String> mapInverse = new TreeMap<IExperiment,String>();
		synchronized(map) {
			for(Entry<String, IExperiment> entry : map.entrySet()) {
				mapInverse.put(entry.getValue(), entry.getKey());
			}
		}
		return mapInverse;
	}
	
	private TreeMap<Long, String> getMapByDate() {
		TreeMap<Long, String> mapByDate = new TreeMap<Long, String>();
		synchronized(map) {
			for(Entry<String, IExperiment> entry : map.entrySet()) {
				IExperiment experiment = entry.getValue();
				Long key = new Long(experiment.getStartTime());
				String value = experiment.getId();
				mapByDate.put(key, value);
			}
		}
		return mapByDate;
	}
	
	public IExperiment getById(String id) {
		IExperiment retVal = null;
		if(id != null) {
			synchronized(map) {
				for(Entry<String, IExperiment> entry : map.entrySet()) {
					IExperiment experiment = entry.getValue();
					if(id.equals(experiment.getId())) {
						retVal = experiment;
						break;
					}
				}
			}
		}
		return retVal;
	}
	
	private void replace(String directory, Experiment experiment) {
		String mName = "replace";
		String id = map.get(directory).getId();
		experiment.setId(id);
		map.put(directory, experiment);
		WsLog.debug(cName, mName, directory);
	}
	
	private void add(String directory, Experiment experiment) {
		String mName = "add";
		map.put(directory, experiment);
		WsLog.debug(cName, mName, directory);
	}
	
	private void put(String directory, Experiment experiment) {
		synchronized(map) {
			if(map.containsKey(directory)) {
				replace(directory, experiment);
			}
			else {
				add(directory, experiment);
			}
		}
	}
	
	private void remove(String directory) {
		String mName = "remove";
		synchronized(map) {
			map.remove(directory);
			
		}
		WsLog.debug(cName, mName, directory);
	}
	
	private boolean containsKey(String directory) {
		synchronized(map) {
			return map.containsKey(directory);
		}
	}
	
	public void initialize(String user, String directory) {
		String mName = "initialize";
		if(user == null) {
			WsLog.warn(cName, mName, "missing user");
		}
		else if(directory == null) {
			WsLog.warn(cName, mName, "missing directory");
		}
		else {
			String parent = ExperimentsRegistryUtilities.upOne(directory);
			update(user, parent, false);
		}
	}
	
	private void update(String user, String directory) {
		String mName = "update";
		try {
			String fileName = ExperimentsRegistryUtilities.getStateFilePath(directory);
			long date = ExperimentsRegistryUtilities.getFileDate(user, fileName);
			WsLog.info(cName, mName, "Reading " + fileName + " date: " + date);
			String contents = ExperimentsRegistryUtilities.getFileContents(user, fileName);
			if(contents != null) {
                // Version may precede the initial '[' and may be on a separate line
			    int version = 0;
			    int offset = contents.indexOf('[');
			    if (offset > 0) {
			        String s = contents.substring(0, offset).trim();
			        try {
			            version = Integer.parseInt(s);
			        } catch (NumberFormatException e) {
			            WsLog.warn(cName, mName, "Invalid version '" + s + "' in state file : " + fileName);
			            return;
			        }
			        contents = contents.substring(offset);
			    }
			    if (offset < 0) {
		             WsLog.warn(cName, mName, "Invalid syntax (missing '[') in state file : " + fileName);
		             return;
			    }
				StringReader sr = new StringReader(contents);
				Type tasksType = new TypeToken<ArrayList<Task>>(){}.getType();
				try {
				    ArrayList<Task> taskArray = gson.fromJson(sr, tasksType);
	                Experiment experiment = new Experiment(user, directory, date, version, taskArray);
	                put(directory, experiment);
                } catch (JsonParseException e) {
                    WsLog.warn(cName, mName,"Ignoring " + fileName + " as has Json syntax error " + e.getMessage());
                }
			}
			else {
				WsLog.trace(cName, mName,"state file missing or inaccessible in "+directory);
				remove(directory);
			}
		}
		catch(Exception e) {
			WsLog.error(cName, mName, e);
		}
	}
	
	private void update(String user, String directory, boolean overwrite) {
		String mName = "update";
		WsLog.enter(cName, mName);
		try {
			if(overwrite) {
				update(user, directory);
			}
			else {
				if(!containsKey(directory)) {
					update(user, directory);
				}
				else {
					WsLog.trace(cName, mName, "duplicate directory: "+directory);
				}
			}
			
		}
		catch(Exception e) {
			WsLog.error(cName, mName, e);
		}
		//WsLog.exit(cName, mName);
	}
	
	private void check() {
		String mName = "check";
		try {
			ArrayList<IExperiment> list = new ArrayList<IExperiment>();
			synchronized(map) {
				for(Entry<String, IExperiment> entry : map.entrySet()) {
					IExperiment experiment = entry.getValue();
					if(experiment.isActive()) {
						list.add(experiment);
					}
				}
			}
			for(IExperiment experiment : list) {
				String user = experiment.getUser();
				String directory = experiment.getDirectory();
				WsLog.debug(cName, mName, "user: "+user+" "+"dirextory: "+directory);
				update(experiment.getUser(), experiment.getDirectory(), true);
			}
		}
		catch(Exception e) {
			WsLog.error(cName, mName, e);
		}
	}
	
	private boolean timeToPrune() {
		boolean retVal = false;
		TreeMap<Long, String> mapByDate = getMapByDate();
		int size = mapByDate.size();
		if(size > MAX_CACHE_SIZE) {
			retVal = true;
		}
		if((updateCounter.get() % 3) == 0) {
			retVal = true;
		}
		return retVal;
	}
	
	private void prune() {
		String mName = "prune";
		WsLog.enter(cName, mName);
		try {
			TreeMap<Long, String> mapByDate = getMapByDate();
			if(timeToPrune()) {
				int cacheCount = 0;
				for(Entry<Long, String> entry : mapByDate.entrySet()) {
					String key = entry.getValue();
					IExperiment experiment = getById(key);
					if(experiment != null) {
						if(experiment.isActive()) {
							cacheCount++;
						}
						else {
							if(cacheCount < MAX_CACHE_SIZE) {
								cacheCount++;
							}
							else {
								String directory = experiment.getDirectory();
								remove(directory);
							}
						}
					}
				}
			}
		}
		catch(Exception e) {
			WsLog.error(cName, mName, e);
		}
		//WsLog.exit(cName, mName);
	}
	
	public void update(IDuccWorkMap dwm) {
		String mName = "update";
		WsLog.enter(cName, mName);
		try {
			if(dwm == null) {
				WsLog.warn(cName, mName, "missing map");
			}
			else {
				updateCounter.incrementAndGet();
				Iterator<DuccId> iterator = dwm.getJobKeySet().iterator();
				while(iterator.hasNext()) {
					DuccId duccId = iterator.next();
					IDuccWork job = dwm.findDuccWork(duccId);
					if(job != null) {
						IDuccStandardInfo stdInfo = job.getStandardInfo();
						if(stdInfo != null) {
							String user = stdInfo.getUser();
							String directory = stdInfo.getLogDirectory();
							String parent = ExperimentsRegistryUtilities.upOne(directory);
							update(user, parent, true);
						}
					}
				}

				// Also process managed reservations in case the experiment has only these.
                iterator = dwm.getManagedReservationKeySet().iterator();
                while(iterator.hasNext()) {
                    DuccId duccId = iterator.next();
                    IDuccWork job = dwm.findDuccWork(duccId);
                    if(job != null) {
                        IDuccStandardInfo stdInfo = job.getStandardInfo();
                        if(stdInfo != null) {
                            String user = stdInfo.getUser();
                            String directory = stdInfo.getLogDirectory();
                            String parent = ExperimentsRegistryUtilities.upOne(directory);
                            update(user, parent, true);
                        }
                    }
                }
			}
			check();
			prune();
		}
		catch(Exception e) {
			WsLog.error(cName, mName, e);
		}
		//WsLog.exit(cName, mName);
	}
}
