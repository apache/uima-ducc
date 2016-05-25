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
package org.apache.uima.ducc.transport.event.common.history;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.uima.ducc.common.DuccEnvironmentHelper;
import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.Pair;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;


public class HistoryPersistenceManager implements IHistoryPersistenceManager {

    
    // private static final DuccLogger logger = DuccLoggerComponents.getTrLogger(HistoryPersistenceManager.class.getName());
    private DuccLogger logger = DuccService.getDuccLogger(HistoryPersistenceManager.class.getName());
    
    private String historyDirectory_jobs = IDuccEnv.DUCC_HISTORY_JOBS_DIR;
    private String historyDirectory_reservations = IDuccEnv.DUCC_HISTORY_RESERVATIONS_DIR;
    private String historyDirectory_services = IDuccEnv.DUCC_HISTORY_SERVICES_DIR;
    
    private String dwj = "dwj";
    private String dwr = "dwr";
    private String dws = "dws";
    
    private enum Verbosity {
        QUIET,
        SPEAK,
    }
    
    HistoryPersistenceManager() {
        mkdirs();
    }
    
    public boolean init(DuccLogger logger)
    {
    	this.logger = logger;
    	return true;
    }
    
    private void mkdirs() {
        IOHelper.mkdirs(historyDirectory_jobs);
        IOHelper.mkdirs(historyDirectory_reservations);
        IOHelper.mkdirs(historyDirectory_services);
    }
    
    private String normalize(String id) {
        String retVal = id;
        return retVal;
    }
    
    private ObjectInputStream getObjectInputStream(FileInputStream fis) throws SecurityException, IOException {
    	ObjectInputStream retVal = null;
    	if(DuccEnvironmentHelper.isTolerateSerialVersionUidMismatch()) {
    		retVal = new DeserializerObjectInputStream(fis);
    	}
    	else {
    		retVal = new ObjectInputStream(fis);
    	}
    	return retVal;
    }  
    
    public void saveJob(IDuccWorkJob duccWorkJob) throws IOException {
        String id = normalize(""+duccWorkJob.getDuccId().getFriendly());
        String fileName = historyDirectory_jobs+File.separator+id+"."+dwj;
        FileOutputStream fos = null;
        ObjectOutputStream out = null;
        fos = new FileOutputStream(fileName);
        out = new ObjectOutputStream(fos);
        out.writeObject(duccWorkJob);
        out.close();
    }
    
    public List<IDuccWorkJob> restoreJobs(long max)
    	throws Exception
    {
    	String methodName = "restoreJobs";
        // Find the 'max' most current jobs.
        // How: list all files; if ends with 'dwj' the first part is the duccid.
        //      put all the duccids into an array and sort it, (ascending so we don't need a comparator)
        //      Starting at the end, call restoreJob(Long) to get the file. 
        //
        List<Long> allJobs = new ArrayList<Long>();
        File folder = new File(historyDirectory_jobs);
        File[] listOfFiles = folder.listFiles();
        if(listOfFiles != null) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    String name = listOfFiles[i].getName();
                    if(name.endsWith("."+dwj)) {
                        int ndx = name.indexOf(".");
                        try {
                            allJobs.add(Long.parseLong(name.substring(0, ndx)));
                        } catch ( NumberFormatException e ) {
                            logger.warn(methodName, null, "Job file", name, ": cannot extract ducc id.  Not restored.");
                        }

                    }
                }
            }
        }

        max = Math.min(allJobs.size(), max);
        Collections.sort(allJobs);
        List<IDuccWorkJob> ret = new ArrayList<IDuccWorkJob>((int)max);
        for ( int i = allJobs.size()-1; i >= 0; i-- ) {
            ret.add(restoreJob(allJobs.get(i)));
        }

        return ret;
    }

    public IDuccWorkJob restoreJob(long duccid)
        throws Exception
    {
        String location = "jobRestore";
        IDuccWorkJob job = null;
        try {
            String fileName = ""+duccid + "." + dwj;
            logger.trace(location, new DuccId(duccid), fileName);
            FileInputStream fis = null;
            ObjectInputStream in = null;
            fis = new FileInputStream(historyDirectory_jobs+File.separator+fileName);
            in = getObjectInputStream(fis);
            job = (IDuccWorkJob) in.readObject();
            in.close();      
        }
        catch(Exception e) {
        	logger.error(location, new DuccId(duccid), e);
        }
        return job;
    }

    // public IDuccWorkJob jobRestore(String fileName) {
    //  return jobRestore(fileName, Verbosity.SPEAK);
    // }

    // private IDuccWorkJob jobRestore(String fileName, Verbosity level) {
    //  String methodName = "jobRestore";
    //  IDuccWorkJob job = null;
    //  try {
    //      logger.trace(methodName, null, "restore:"+fileName);
    //      FileInputStream fis = null;
    //      ObjectInputStream in = null;
    //      fis = new FileInputStream(historyDirectory_jobs+File.separator+fileName);
    //      in = new ObjectInputStream(fis);
    //      job = (IDuccWorkJob) in.readObject();
    //      in.close();
    //  }
    //  catch(Exception e) {
    //      switch(level) {
    //      case QUIET:
    //          break;
    //      case SPEAK:
    //          logger.warn(methodName, null, "unable to restore:"+fileName, e);
    //          break;
    //      }
    //  }
    //  return job;
    // }
    
    
    // public IDuccWorkJob jobRestore(DuccId duccId) {
    //  String fileName = duccId.getFriendly()+"."+dwj;
    //  return jobRestore(fileName, Verbosity.QUIET);
    // }
    
    
    //    public ArrayList<Long> jobList() { return null; }
//  public ArrayList<String> jobList() {
//      ArrayList<String> retVal = new ArrayList<String>();
//      File folder = new File(historyDirectory_jobs);
//      File[] listOfFiles = folder.listFiles();
//      if(listOfFiles != null) {
//          for (int i = 0; i < listOfFiles.length; i++) {
//              if (listOfFiles[i].isFile()) {
//                  String name = listOfFiles[i].getName();
//                  if(name.endsWith("."+dwj)) {
//                      retVal.add(name);
//                  }
//              }
//          }
//      }
//      return retVal;
//  }
    
    
    // public ArrayList<IDuccWorkJob> jobRestore() throws IOException, ClassNotFoundException {
    //  ArrayList<IDuccWorkJob> retVal = new ArrayList<IDuccWorkJob>();
    //  ArrayList<String> jobFileNames = jobList();
    //  ListIterator<String> listIterator = jobFileNames.listIterator();
    //  while(listIterator.hasNext()) {
    //      String fileName = listIterator.next();
    //      IDuccWorkJob job = jobRestore(fileName);
    //      if(job != null) {
    //          retVal.add(job);
    //      }
    //  }
    //  return retVal;
    // }

    // public ArrayList<IDuccWorkJob> jobRestore() throws IOException, ClassNotFoundException 
    // {
    //     return null;
    // }

    
//    public void reservationSaveConditional(IDuccWorkReservation duccWorkReservation) throws IOException {
//        String id = normalize(""+duccWorkReservation.getDuccId().getFriendly());
//        String fileName = historyDirectory_jobs+File.separator+id+"."+dwr;
//        File file = new File(fileName);
//        if(!file.exists()) {
//            reservationSave(duccWorkReservation);
//        }
//    }
    
    
    public void saveReservation(IDuccWorkReservation duccWorkReservation) 
        throws Exception 
    {
        String id = normalize(""+duccWorkReservation.getDuccId().getFriendly());
        String fileName = historyDirectory_reservations+File.separator+id+"."+dwr;
        FileOutputStream fos = null;
        ObjectOutputStream out = null;
        fos = new FileOutputStream(fileName);
        out = new ObjectOutputStream(fos);
        out.writeObject(duccWorkReservation);
        out.close();
    }
    
    
    public IDuccWorkReservation restoreReservation(long duccid)
        throws Exception
    {
        String location = "reservationRestore";
        IDuccWorkReservation reservation = null;
        try {
        	FileInputStream fis = null;
            ObjectInputStream in = null;
            String fileName = ""+duccid + "." + dwr;
            logger.trace(location, new DuccId(duccid), fileName);
            fis = new FileInputStream(historyDirectory_reservations+File.separator+fileName);
            in = getObjectInputStream(fis);
            reservation = (IDuccWorkReservation) in.readObject();
            in.close();
        }
        catch(Exception e) {
        	logger.error(location, new DuccId(duccid), e);
        }
        return reservation;
    }

    public List<IDuccWorkReservation> restoreReservations(long max)
    	throws Exception
    {
    	String methodName = "restoreReservations";
        // Find the 'max' most current jobs.
        // How: list all files; if ends with 'dwj' the first part is the duccid.
        //      put all the duccids into an array and sort it, (ascending so we don't need a comparator)
        //      Starting at the end, call restoreJob(Long) to get the file. 
        //
        List<Long> allReservations = new ArrayList<Long>();
        File folder = new File(historyDirectory_reservations);
        File[] listOfFiles = folder.listFiles();
        if(listOfFiles != null) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    String name = listOfFiles[i].getName();
                    if(name.endsWith("."+dwr)) {
                        int ndx = name.indexOf(".");
                        try {
                            allReservations.add(Long.parseLong(name.substring(0, ndx)));
                        } catch ( NumberFormatException e ) {
                            logger.warn(methodName, null, "Reservation file", name, ": cannot extract ducc id.  Not restored.");
                        }

                    }
                }
            }
        }

        max = Math.min(allReservations.size(), max);
        Collections.sort(allReservations);
        List<IDuccWorkReservation> ret = new ArrayList<IDuccWorkReservation>((int)max);
        for ( int i = allReservations.size()-1; i >= 0; i-- ) {
            ret.add(restoreReservation(allReservations.get(i)));
        }

        return ret;
    }

    
    // private IDuccWorkReservation reservationRestore(String fileName, Verbosity level) {
    //     String methodName = "reservationRestore";
    //     IDuccWorkReservation reservation = null;
    //     try {
    //         logger.trace(methodName, null, "restore:"+fileName);
    //         FileInputStream fis = null;
    //         ObjectInputStream in = null;
    //         fis = new FileInputStream(historyDirectory_reservations+File.separator+fileName);
    //         in = new ObjectInputStream(fis);
    //         reservation = (IDuccWorkReservation) in.readObject();
    //         in.close();
    //     }
    //     catch(Exception e) {
    //         switch(level) {
    //         case QUIET:
    //             break;
    //         case SPEAK:
    //             logger.warn(methodName, null, "unable to restore:"+fileName);
    //             break;
    //         }
    //     }
    //     return reservation;
    // }
    
    
    // public ArrayList<String> reservationList() {
    //     ArrayList<String> retVal = new ArrayList<String>();
    //     File folder = new File(historyDirectory_reservations);
    //     File[] listOfFiles = folder.listFiles();
    //     if(listOfFiles != null) {
    //         for (int i = 0; i < listOfFiles.length; i++) {
    //             if (listOfFiles[i].isFile()) {
    //                 String name = listOfFiles[i].getName();
    //                 if(name.endsWith("."+dwr)) {
    //                     retVal.add(name);
    //                 }
    //             }
    //         }
    //     }
    //     return retVal;
    // }
    
    
    // public ArrayList<IDuccWorkReservation> reservationRestore() throws IOException, ClassNotFoundException {
    //     ArrayList<IDuccWorkReservation> retVal = new ArrayList<IDuccWorkReservation>();
    //     ArrayList<String> reservationFileNames = reservationList();
    //     ListIterator<String> listIterator = reservationFileNames.listIterator();
    //     while(listIterator.hasNext()) {
    //         String fileName = listIterator.next();
    //         IDuccWorkReservation reservation = reservationRestore(fileName);
    //         if(reservation != null) {
    //             retVal.add(reservation);
    //         }
    //     }
    //     return retVal;
    // }
    
    
    // public IDuccWorkReservation reservationRestore(DuccId duccId) {
    //     String fileName = duccId.getFriendly()+"."+dwr;
    //     return reservationRestore(fileName, Verbosity.QUIET);
    // }


    public void saveService(IDuccWorkService duccWorkService)
        throws Exception 
    {
        String id = normalize(""+duccWorkService.getDuccId().getFriendly());
        String fileName = historyDirectory_services+File.separator+id+"."+dws;
        FileOutputStream fos = null;
        ObjectOutputStream out = null;
        fos = new FileOutputStream(fileName);
        out = new ObjectOutputStream(fos);
        out.writeObject(duccWorkService);
        out.close();
    }


    public IDuccWorkService restoreService(long duccid)
        throws Exception
    {
        String location = "restoreService";
        IDuccWorkService service = null;
        try {
        	FileInputStream fis = null;
            ObjectInputStream in = null;
            String fileName = ""+duccid + "." + dws;
            logger.trace(location, new DuccId(duccid), fileName);
            fis = new FileInputStream(historyDirectory_services+File.separator+fileName);
            in = getObjectInputStream(fis);
            service = (IDuccWorkService) in.readObject();
            in.close();
        }
        catch(Exception e) {
        	logger.error(location, new DuccId(duccid), e);
        }
        return service;
    }

    public List<IDuccWorkService> restoreServices(long max)
    	throws Exception
    {
    	String methodName = "restoreServices";
        // Find the 'max' most current jobs.
        // How: list all files; if ends with 'dwj' the first part is the duccid.
        //      put all the duccids into an array and sort it, (ascending so we don't need a comparator)
        //      Starting at the end, call restoreJob(Long) to get the file. 
        //
        List<Long> allServices = new ArrayList<Long>();
        File folder = new File(historyDirectory_services);
        File[] listOfFiles = folder.listFiles();
        if(listOfFiles != null) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    String name = listOfFiles[i].getName();
                    if(name.endsWith("."+dws)) {
                        int ndx = name.indexOf(".");
                        try {
                            allServices.add(Long.parseLong(name.substring(0, ndx)));
                        } catch ( NumberFormatException e ) {
                            logger.warn(methodName, null, "Reservation file", name, ": cannot extract ducc id.  Not restored.");
                        }

                    }
                }
            }
        }

        max = Math.min(allServices.size(), max);
        Collections.sort(allServices);
        List<IDuccWorkService> ret = new ArrayList<IDuccWorkService>((int)max);
        for ( int i = allServices.size()-1; i >= 0; i-- ) {
            ret.add(restoreService(allServices.get(i)));
        }

        return ret;
    }
    
    // public void serviceSaveConditional(IDuccWorkService duccWorkService)
    //         throws IOException {
    //     String id = normalize(""+duccWorkService.getDuccId().getFriendly());
    //     String fileName = historyDirectory_services+File.separator+id+"."+dws;
    //     File file = new File(fileName);
    //     if(!file.exists()) {
    //         serviceSave(duccWorkService);
    //     }
    // }

    

    public void serviceSave(IDuccWorkService duccWorkService)
            throws IOException {
        String id = normalize(""+duccWorkService.getDuccId().getFriendly());
        String fileName = historyDirectory_services+File.separator+id+"."+dws;
        FileOutputStream fos = null;
        ObjectOutputStream out = null;
        fos = new FileOutputStream(fileName);
        out = new ObjectOutputStream(fos);
        out.writeObject(duccWorkService);
        out.close();
    }

    
    public IDuccWorkService serviceRestore(String fileName) {
        return serviceRestore(fileName, Verbosity.SPEAK);
    }
    
    private IDuccWorkService serviceRestore(String fileName, Verbosity level) {
        String methodName = "serviceRestore";
        IDuccWorkService service = null;
        try {
            logger.trace(methodName, null, "restore:"+fileName);
            FileInputStream fis = null;
            ObjectInputStream in = null;
            fis = new FileInputStream(historyDirectory_services+File.separator+fileName);
            in = getObjectInputStream(fis);
            service = (IDuccWorkService) in.readObject();
            in.close();
        }
        catch(Exception e) {
            switch(level) {
            case QUIET:
                break;
            case SPEAK:
                logger.warn(methodName, null, "unable to restore:"+fileName);
                break;
            }
        }
        return service;
    }

    
    public ArrayList<String> serviceList() {
        ArrayList<String> retVal = new ArrayList<String>();
        File folder = new File(historyDirectory_services);
        File[] listOfFiles = folder.listFiles();
        if(listOfFiles != null) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    String name = listOfFiles[i].getName();
                    if(name.endsWith("."+dws)) {
                        retVal.add(name);
                    }
                }
            }
        }
        return retVal;
    }

    
    public ArrayList<IDuccWorkService> serviceRestore() throws IOException,
            ClassNotFoundException {
        ArrayList<IDuccWorkService> retVal = new ArrayList<IDuccWorkService>();
        ArrayList<String> serviceFileNames = serviceList();
        ListIterator<String> listIterator = serviceFileNames.listIterator();
        while(listIterator.hasNext()) {
            String fileName = listIterator.next();
            IDuccWorkService service = serviceRestore(fileName);
            if(service != null) {
                retVal.add(service);
            }
        }
        return retVal;
    }

    
    public IDuccWorkService serviceRestore(DuccId duccId) {
        String fileName = duccId.getFriendly()+"."+dws;
        return serviceRestore(fileName, Verbosity.QUIET);
    }
    
    public boolean checkpoint(DuccWorkMap m, Map<DuccId, DuccId> processToJob)
        throws Exception
    {
        return false;
    }

    public Pair<DuccWorkMap, Map<DuccId, DuccId>> restore()
        throws Exception
    {
        return null;
    }

    private static int doJobs(HistoryPersistenceManager hpm) 
    		throws Exception
    {
        List<IDuccWorkJob> duccWorkJobs = hpm.restoreJobs(-1L);
        ListIterator<IDuccWorkJob> listIterator = duccWorkJobs.listIterator();
        int acc = 0;
        while(listIterator.hasNext()) {
            IDuccWorkJob duccWorkJob = listIterator.next();
            System.out.println(duccWorkJob.getId());
            acc++;
        }
        return acc;
    }
    
    private static int doReservations(HistoryPersistenceManager hpm) 
    		throws Exception
    {
        List<IDuccWorkReservation> duccWorkReservations = hpm.restoreReservations(-1L);
        ListIterator<IDuccWorkReservation> listIterator = duccWorkReservations.listIterator();
        int acc = 0;
        while(listIterator.hasNext()) {
            IDuccWorkReservation duccWorkReservation = listIterator.next();
            System.out.println(duccWorkReservation.getId());
            acc++;
        }
        return acc;
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String ducc_home = Utils.findDuccHome();
        if(ducc_home == null) {
            System.out.println("DUCC_HOME not set in environment");
            return;
        }
        if(ducc_home.trim() == "") {
            System.out.println("DUCC_HOME not set in environment");
            return;
        }
        int reservations = 0;
		try {
			HistoryPersistenceManager hpm = new HistoryPersistenceManager();
			int jobs = doJobs(hpm);
			System.out.println("jobs: "+jobs);
			reservations = doReservations(hpm);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("reservations: "+reservations);
    }

    ///// </tool>

}
