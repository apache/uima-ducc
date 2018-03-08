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
package org.apache.uima.ducc.ws;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.history.HistoryFactory;
import org.apache.uima.ducc.transport.event.common.history.IHistoryPersistenceManager;


public class DuccBoot extends Thread {
	
	private static DuccLogger logger = DuccLogger.getLogger(DuccBoot.class);
	private static Messages messages = Messages.getInstance();
	
	private static DuccPlugins duccPlugins = DuccPlugins.getInstance();
	
	private static DuccId jobid = null;
	
	public static long maxJobs = 4096;
	public static long maxReservations = 4096;
	public static long maxServices = 4096;
	
	public static void boot() {
		DuccBoot duccBoot = new DuccBoot();
		duccBoot.initialize();
		duccBoot.start();
	}

	private HashSet<String> experimentsFound;
	
	public void run() {
		String location = "run";
		try {
			logger.info(location, jobid, "booting...");
			restore();
			logger.info(location, jobid, "ready.");
		}
		catch(Throwable t) {
			logger.error(location, jobid, t);
		}
		
	}
	
	private static long getLimit() {
		long limit = 0;
		try {
			String p_limit = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_ws_max_history_entries);
			if(p_limit != null) {
				p_limit = p_limit.trim();
				if(!p_limit.equals("unlimited")) {
					limit = Long.parseLong(p_limit);
				}
			}
		}
		catch(Throwable t) {
			t.printStackTrace();
		}
		// limit = 0;
		return limit;
	}
	
	
	// private void restoreReservations(IHistoryPersistenceManager hpm, DuccData duccData) {
	// 	String location = "restoreReservations";
	// 	ArrayList<String> duccWorkReservations = hpm.reservationList();
	// 	logger.info(location, jobid, messages.fetchLabel("Number of Reservations to restore")+duccWorkReservations.size());
	// 	TreeMap<Integer,String> map = sort(duccWorkReservations);
	// 	Iterator<Integer> iterator = map.descendingKeySet().iterator();
	// 	int i = 0;
	// 	int restored = 0;
	// 	while(iterator.hasNext() && (++i < maxReservations)) {
	// 		try {
	// 			Integer key = iterator.next();
	// 			logger.debug(location, jobid, messages.fetchLabel("restore")+key);
	// 			String fileName = map.get(key);
	// 			IDuccWorkReservation duccWorkReservation;
	// 			duccWorkReservation = hpm.reservationRestore(fileName);
	// 			if(duccWorkReservation != null) {
	// 				duccData.putIfNotPresent(duccWorkReservation);
	// 				duccPlugins.restore(duccWorkReservation);
	// 				restored++;
	// 			}
	// 		}
	// 		catch(Throwable t) {
	// 			logger.warn(location, jobid, t);
	// 		}
	// 	}
	// 	logger.info(location, jobid, messages.fetch("Reservations restored: "+restored));
	// }
	
	private void restoreReservations(IHistoryPersistenceManager hpm, DuccData duccData) 
    {
        // Replaced for database.  Both file and database now do all the looping and sorting internally.
        String location = "restoreReservations";
        List<IDuccWorkReservation> duccWorkReservations = null;;
		try {
			duccWorkReservations = hpm.restoreReservations(maxReservations);
		} catch (Exception e) {
            logger.warn(location, null, e);
            return;                               // Nothing to do if this fails
		}

        logger.info(location, jobid, messages.fetchLabel("Number of Reservations fetched from history"), duccWorkReservations.size());

        int restored = 0;
        int nExperiments = 0;
        for ( IDuccWorkReservation duccWorkReservation : duccWorkReservations ) {
            try {
                logger.debug(location, duccWorkReservation.getDuccId(), messages.fetchLabel("restore"));
                duccData.putIfNotPresent(duccWorkReservation);
                String directory = duccWorkReservation.getStandardInfo().getLogDirectory();
                if (experimentsFound.add(directory)) {
                    duccPlugins.restore(duccWorkReservation);
                    nExperiments++;
                }
                restored++;
            }
            catch(Throwable t) {
                logger.warn(location, duccWorkReservation.getDuccId(), t);
            }
        }
        logger.info(location,null, messages.fetch("Reservations restored: "+restored));
        if (nExperiments > 0) {
        	logger.info(location,null, messages.fetch("Experiments found: "+nExperiments));
        }
    }
    
	/**
	 * Verify user log dir is correct and fix-up (in WS cache) if not.
	 * 
	 * See https://issues.apache.org/jira/browse/UIMA-5506
	 */
    private void fixup(IDuccWork dw) {
    	String location = "fixup";
    	if(dw != null) {
    		DuccId duccId = dw.getDuccId();
    		try {
    			if(dw instanceof DuccWorkJob) {
    				DuccWorkJob dwj = (DuccWorkJob) dw;
    				String s_logdir = dwj.getLogDirectory();
    				String s_duccId = duccId.toString();
    				File f_userlogdir = new File(s_logdir, s_duccId);
    				String s_userlogdir = f_userlogdir.getPath()+File.separator;
    				String h_userlogdir = dwj.getUserLogDir();
    				if(s_userlogdir.equals(h_userlogdir)) {
    					logger.debug(location, duccId, "==", h_userlogdir, s_userlogdir);
    				}
    				else {
    					logger.debug(location, duccId, "!=", h_userlogdir, s_userlogdir);
    					Field field = DuccWorkJob.class.getDeclaredField("userLogDir");
    					field.setAccessible(true); // Force to access the field
    					field.set(dwj, s_userlogdir); // Set value
    					logger.info(location, duccId, dwj.getUserLogDir());
    				}
    			}
    		}
    		catch(Exception e) {
    			logger.error(location, duccId, e);
    		}
    	}
    }
	
	private void restoreJobs(IHistoryPersistenceManager hpm, DuccData duccData) 
    {
        // Replaced for database.  Both file and database now do all the looping and sorting internally.
        String location = "restoreJobs";
        List<IDuccWorkJob> duccWorkJobs = null;
		try {
			duccWorkJobs = hpm.restoreJobs(maxJobs);
		} catch (Exception e) {
            logger.warn(location, null, e);
            return;                               // Nothing to do if this fails
		}

        logger.info(location, jobid, messages.fetchLabel("Number of Jobs fetched from history"), duccWorkJobs.size());

        int restored = 0;
        int nExperiments = 0;
        for ( IDuccWorkJob duccWorkJob : duccWorkJobs ) {
        	fixup(duccWorkJob);
            try {
                logger.debug(location, duccWorkJob.getDuccId(), messages.fetchLabel("restore"));
                duccData.putIfNotPresent(duccWorkJob);
                String directory = duccWorkJob.getStandardInfo().getLogDirectory();
                if (experimentsFound.add(directory)) {
                    duccPlugins.restore(duccWorkJob);
                    nExperiments++;
                }
                restored++;
            }
            catch(Throwable t) {
                logger.warn(location, duccWorkJob.getDuccId(), t);
            }
        }
        logger.info(location,null, messages.fetch("Jobs restored: "+restored));
        if (nExperiments > 0) {
        	logger.info(location,null, messages.fetch("Experiments found: "+nExperiments));
        }
    }
	
//	private void restoreJobsX(IHistoryPersistenceManager hpm, DuccData duccData) {
//		String location = "restoreJobs";
//		ArrayList<String> duccWorkJobs = hpm.jobList();
//		logger.info(location, jobid, messages.fetchLabel("Number of Jobs to restore")+duccWorkJobs.size());
//		TreeMap<Integer,String> map = sort(duccWorkJobs);
//		Iterator<Integer> iterator = map.descendingKeySet().iterator();
//		int i = 0;
//		int restored = 0;
//		while(iterator.hasNext() && (++i < maxJobs)) {
//			try {
//				Integer key = iterator.next();
//				logger.debug(location, jobid, messages.fetchLabel("restore")+key);
//				String fileName = map.get(key);
//				IDuccWorkJob duccWorkJob;
//				duccWorkJob = hpm.jobRestore(fileName);
//				if(duccWorkJob != null) {
//					duccData.putIfNotPresent(duccWorkJob);
//					duccPlugins.restore(duccWorkJob);
//					restored++;
//				}
//			}
//			catch(Throwable t) {
//				logger.warn(location, jobid, t);
//			}
//		}
//		logger.info(location, jobid, messages.fetch("Jobs restored: "+restored));
//	}

	private void restoreServices(IHistoryPersistenceManager hpm, DuccData duccData) 
    {
        // Replaced for database.  Both file and database now do all the looping and sorting internally.
        String location = "restoreServices";
        List<IDuccWorkService> duccWorkServices = null;;
		try {
			duccWorkServices = hpm.restoreServices(maxServices);
		} catch (Exception e) {
            logger.warn(location, null, e);
            return;                               // Nothing to do if this fails
		}

        logger.info(location, jobid, messages.fetchLabel("Number of services fetched from history"), duccWorkServices.size());

        int restored = 0;
        int nExperiments = 0;
        for ( IDuccWorkService duccWorkService : duccWorkServices ) {
            try {
                logger.debug(location, duccWorkService.getDuccId(), messages.fetchLabel("restore"));
                duccData.putIfNotPresent(duccWorkService);
                String directory = duccWorkService.getStandardInfo().getLogDirectory();
                if (experimentsFound.add(directory)) {
                    duccPlugins.restore(duccWorkService);
                    nExperiments++;
                }
                restored++;
            }
            catch(Throwable t) {
                logger.warn(location, duccWorkService.getDuccId(), t);
            }
        }
        logger.info(location,null, messages.fetch("Services restored: "+restored));
        if (nExperiments > 0) {
        	logger.info(location,null, messages.fetch("Experiments found: "+nExperiments));
        }
    }
	
	private void restoreArbitraryProcesses(IHistoryPersistenceManager hpm, DuccData duccData) 
    {
        // Replaced for database.  Both file and database now do all the looping and sorting internally.
        String location = "restoreArbitraryProcesses";
        List<IDuccWorkService> duccWorkServices = null;;
		try {
			duccWorkServices = hpm.restoreArbitraryProcesses(maxServices);
		} catch (Exception e) {
            logger.warn(location, null, e);
            return;                               // Nothing to do if this fails
		}

        logger.info(location, jobid, messages.fetchLabel("Number of services fetched from history"), duccWorkServices.size());

        int restored = 0;
        int nExperiments = 0;
        for ( IDuccWorkService duccWorkService : duccWorkServices ) {
            try {
                logger.debug(location, duccWorkService.getDuccId(), messages.fetchLabel("restore"));
                duccData.putIfNotPresent(duccWorkService);
                String directory = duccWorkService.getStandardInfo().getLogDirectory();
                if (experimentsFound.add(directory)) {
                    duccPlugins.restore(duccWorkService);
                    nExperiments++;
                }
                restored++;
            }
            catch(Throwable t) {
                logger.warn(location, duccWorkService.getDuccId(), t);
            }
        }
        logger.info(location,null, messages.fetch("Services restored: "+restored));
        if (nExperiments > 0) {
        	logger.info(location,null, messages.fetch("Experiments found: "+nExperiments));
        }
    }
	
	// private void restoreServices(IHistoryPersistenceManager hpm, DuccData duccData) {
	// 	String location = "restoreServices";
	// 	ArrayList<String> duccWorkServices = hpm.serviceList();
	// 	logger.info(location, jobid, messages.fetchLabel("Number of Services to restore")+duccWorkServices.size());
	// 	TreeMap<Integer,String> map = sort(duccWorkServices);
	// 	Iterator<Integer> iterator = map.descendingKeySet().iterator();
	// 	int i = 0;
	// 	int restored = 0;
	// 	while(iterator.hasNext() && (++i < maxServices)) {
	// 		try {
	// 			Integer key = iterator.next();
	// 			logger.debug(location, jobid, messages.fetchLabel("restore")+key);
	// 			String fileName = map.get(key);
	// 			IDuccWorkService duccWorkService;
	// 			duccWorkService = hpm.serviceRestore(fileName);
	// 			if(duccWorkService != null) {
	// 				duccData.putIfNotPresent(duccWorkService);
	// 				duccPlugins.restore(duccWorkService);
	// 				restored++;
	// 			}
	// 		}
	// 		catch(Throwable t) {
	// 			logger.warn(location, jobid, t);
	// 		}
	// 	}
	// 	logger.info(location, jobid, messages.fetch("Services restored: "+restored));
	// }
	
	private void initialize() {
		String location = "initialize";
		long limit = getLimit();
		if(limit > 0) {
			logger.info(location, jobid, messages.fetchLabel("max history limit")+limit);
			maxJobs = limit;
			maxReservations = limit;
			maxServices = limit;
		}
	}
	
	private void restore() {
		String location = "restore";
		logger.info(location, jobid, messages.fetchLabel("History directory")+IDuccEnv.DUCC_HISTORY_DIR);
		IHistoryPersistenceManager hpm = HistoryFactory.getInstance(this.getClass().getName());
		DuccData.reset();
		DuccData duccData = DuccData.getInstance();
		experimentsFound = new HashSet<String>();  // Lets the restore methods avoid inspecting already-found experiments
		restoreReservations(hpm, duccData);
		restoreJobs(hpm, duccData);
		restoreServices(hpm, duccData);
		restoreArbitraryProcesses(hpm, duccData);
		experimentsFound = null;
		duccData.report();
	}
}
