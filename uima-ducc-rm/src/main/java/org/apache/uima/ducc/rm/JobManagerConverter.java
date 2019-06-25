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
package org.apache.uima.ducc.rm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils.DuccMapDifference;
import org.apache.uima.ducc.common.utils.DuccCollectionUtils.DuccMapValueDifference;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.rm.scheduler.IJobManager;
import org.apache.uima.ducc.rm.scheduler.IRmJob;
import org.apache.uima.ducc.rm.scheduler.ISchedulerMain;
import org.apache.uima.ducc.rm.scheduler.JobManagerUpdate;
import org.apache.uima.ducc.rm.scheduler.Machine;
import org.apache.uima.ducc.rm.scheduler.ResourceClass;
import org.apache.uima.ducc.rm.scheduler.RmJob;
import org.apache.uima.ducc.rm.scheduler.SchedConstants;
import org.apache.uima.ducc.rm.scheduler.SchedulingException;
import org.apache.uima.ducc.rm.scheduler.Share;
import org.apache.uima.ducc.transport.event.RmStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccPerWorkItemStatistics;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessMap;
import org.apache.uima.ducc.transport.event.common.IDuccReservation;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkExecutable;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkMap;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.ITimeWindow;
import org.apache.uima.ducc.transport.event.rm.IResource;
import org.apache.uima.ducc.transport.event.rm.IRmJobState;
import org.apache.uima.ducc.transport.event.rm.Resource;
import org.apache.uima.ducc.transport.event.rm.RmJobState;


/**
 * Convert the scheduler's structures into the events that get returned to the world.
 */

public class JobManagerConverter
    implements IJobManager,
    	SchedConstants
{
    DuccLogger logger = DuccLogger.getLogger(JobManagerConverter.class, COMPONENT_NAME);
    ISchedulerMain scheduler;
    NodeStability nodeStability = null;

    DuccWorkMap localMap = null;
    JobManagerUpdate lastJobManagerUpdate = new JobManagerUpdate();

    Map<IRmJob, IRmJob> refusedJobs = new HashMap<IRmJob, IRmJob>();

    Map<DuccId, IRmJobState> blacklistedResources = new HashMap<DuccId, IRmJobState>(); // UIMA-4142 to tell OR

    boolean recovery = false;

    public JobManagerConverter(ISchedulerMain scheduler, NodeStability ns)
    {
        this.scheduler = scheduler;
        this.localMap = new DuccWorkMap();
        this.nodeStability = ns;

        DuccLogger.setUnthreaded();

        recovery = SystemPropertyResolver.getBooleanProperty("ducc.rm.fast.recovery", true);
    }

    int toInt(String s, int deflt)
    {
        try {
            int val = Integer.parseInt(s);
            return ( val == 0 ) ? deflt : val;
        } catch ( Throwable t ) {
            return deflt;
        }
    }
  
    // UIMA-4712
    long toLong(String s, long deflt)
    {
        try {
            long val = Long.parseLong(s);
            return ( val == 0L ) ? deflt : val;
        } catch ( Throwable t ) {
            return deflt;
        }
    }
  
    void refuse(IRmJob j, String reason)
    {
        j.refuse(reason);
        synchronized(refusedJobs) {
            refusedJobs.put(j, j);
        }
    }

    /**
     * Purge everything in the world for this job.
     * UIMA-4142
     */
    void blacklistJob(IDuccWork job, long memory, boolean evict)
    {
    	String methodName = "blacklistJob";
        
        Map<DuccId, IResource> all_shares      = null; 
        Map<DuccId, IResource> shrunken_shares = null;
        Map<DuccId, IResource> expanded_shares = null;

        if ( evict ) {
            all_shares      = new LinkedHashMap<DuccId, IResource>();
            shrunken_shares = new LinkedHashMap<DuccId, IResource>();
            expanded_shares = new LinkedHashMap<DuccId, IResource>();
        }

        // first time - everything must go
        IDuccProcessMap pm = ((IDuccWorkExecutable)job).getProcessMap();              
        int quantum = 0;
        for ( IDuccProcess proc : pm.values() ) {          // build up Shares from the incoming state
            NodeIdentity ni = proc.getNodeIdentity();
            Machine m = scheduler.getMachine(ni);
            int share_order = 1;
            
            if ( m != null ) {
            	quantum = m.getQuantum();
                if ( proc.isActive() || (proc.getProcessState() == ProcessState.Undefined) ) {                                    
                    logger.info(methodName, job.getDuccId(), "blacklist", proc.getDuccId(), 
                                "state", proc.getProcessState(), "isActive", proc.isActive(), "isComplete", proc.isComplete());
                    m.blacklist(job.getDuccId(), proc.getDuccId(), memory);
                    if ( evict ) {
                        share_order = m.getShareOrder();         // best guess
                        Resource r = new Resource(proc.getDuccId(), proc.getNode(), false, share_order, 0);
                        all_shares.put(proc.getDuccId(), r);
                        shrunken_shares.put(proc.getDuccId(), r);
                    }
                } else {
                    logger.info(methodName, job.getDuccId(), "whitelist", proc.getDuccId(), 
                                "state", proc.getProcessState(), "isActive", proc.isActive(), "isComplete", proc.isComplete());
                    m.whitelist(proc.getDuccId());
                }
            }
        }

        if ( evict && (shrunken_shares.size() > 0) ) {
            RmJobState rjs = new RmJobState(job.getDuccId(), quantum, all_shares, shrunken_shares, expanded_shares);
            rjs.setDuccType(job.getDuccType());
            blacklistedResources.put(job.getDuccId(), rjs);          // to tell OR
        }
    }

    // UIMA-4142
    void blacklistReservation(IDuccWork job)
    {
    	String methodName = "blacklistReservation";
        logger.trace(methodName, job.getDuccId(), "enter");

        IDuccReservationMap drm = ((IDuccWorkReservation) job).getReservationMap();
                
        for ( IDuccReservation idr : drm.values() ) {
            NodeIdentity ni = idr.getNodeIdentity();
            Machine m = scheduler.getMachine(ni);
            if ( m == null ) {                             // not known, huh? maybe next epoch it will have checked in
                logger.warn(methodName, job.getDuccId(), "Problem whitelisting: cannot find machine", ni.getCanonicalName());
            } else {
                m.blacklist(job.getDuccId(), idr.getDuccId(), -1);
            }
        }

    }

    // UIMA-4142
    void blacklist(IDuccWork job, int memory)
    {
        String methodName = "blacklist";
        logger.trace(methodName, job.getDuccId(), "enter");

        switch ( job.getDuccType() ) {
            case Job:
                blacklistJob(job, memory, true);
                break;
            case Service:
            case Pop:
                switch ( ((IDuccWorkService)job).getServiceDeploymentType() ) 
                    {
                    case uima:
                    case custom:
                        blacklistJob(job, memory, true);
                        break;
                    case other:
                        blacklistJob(job, memory, false);
                        break;
                    }
                break;
            case Reservation:
                blacklistReservation(job);
                break;
            default:
                // illegal - internal error if this happens
                logger.error(methodName, job.getDuccId(), "Unknown job type", job.getDuccType(), "ignoring in blacklist.");
                break;                    
        }
    }

    // UIMA-4142
    void whitelist(IDuccWork job)
    {
    	String methodName = "whitelist";

        switch ( job.getDuccType() ) {
            case Job:                
            case Service:
            case Pop:
                for ( IDuccProcess idp : ((IDuccWorkJob) job).getProcessMap().values() ) {
                    NodeIdentity ni = idp.getNodeIdentity();
                    Machine m = scheduler.getMachine(ni);
                    if ( m == null ) {                             // not known, huh? maybe next epoch it will have checked in
                        logger.warn(methodName, job.getDuccId(), "Problem whitelisting: cannot find machine", ni.getCanonicalName());
                    } else {
                        m.whitelist(idp.getDuccId());
                    }
                }

                break;
            case Reservation:
                for ( IDuccReservation idp : ((IDuccWorkReservation) job).getReservationMap().values() ) {
                    NodeIdentity ni = idp.getNodeIdentity();
                    Machine m = scheduler.getMachine(ni);
                    if ( m == null ) {                             // not known, huh? maybe next epoch it will have checked in
                        logger.warn(methodName, job.getDuccId(), "Problem whitelisting: cannot find machine", ni.getCanonicalName());
                    } else {
                        m.whitelist(idp.getDuccId());
                    }
                }
                break;
            default:
                // illegal - internal error if this happens
                logger.error(methodName, job.getDuccId(), "Unknown job type", job.getDuccType(), "ignoring in blacklist.");
                break;                    
        }
    }

    String getElapsedTime(ITimeWindow w)
    {
        if ( w == null ) return "0";
        return w.getDiff();
    }

    /**
     * UIMA-4142
     * Work out if the job is being recovered.  How do you know?  Because
     *   A) for non-reservation, there is a process map and the job is not completed
     *   B) there is a reservation map and the job is not completed
     *
     * The maps are built up from Share assignments earlier.
     */
    boolean isRecovered(IDuccWork job)
    {
        switch ( job.getDuccType() ) {
          case Service:
          case Pop:
          case Job:              
              IDuccProcessMap     pm = ((IDuccWorkExecutable)job).getProcessMap();              
              return ( (pm.size() > 0) && !job.isCompleted() );
          case Reservation:
              IDuccReservationMap rm = ((IDuccWorkReservation)job).getReservationMap();
              return ( (rm.size() > 0) && !job.isCompleted() );
        }
        throw new IllegalStateException("Cannot recognize job type for " + job.getDuccId() + ": found " + job.getDuccType());
    }

//    void formatSchedulingInfo(DuccId id, IDuccSchedulingInfo si, int remaining_work)
//    {
//    	String methodName = "formatSchedulingInfo";
//        SynchronizedDescriptiveStatistics stats = si.getPerWorkItemProcessingTime();        
//        double arith_mean = stats.getMean();
//        double geom_mean = stats.getGeometricMean();
//        double[] vals = stats.getSortedValues();
//        
//        logger.info(methodName, null, id, "STATS: arithmetic mean:", arith_mean);
//        logger.info(methodName, null, id, "STATS: geometric  mean:", geom_mean);
//        logger.info(methodName, null, id, "STATS: remaining  work:", remaining_work);
//        logger.info(methodName, null, id, "STATS: nvals          :", vals.length);
//        
//        if ( vals.length > 0 ) {
//            StringBuffer buf = new StringBuffer();
//            int cnt = 0;
//         
//            for ( int i = 0; i < vals.length; i++ ) {
//                buf.append(Double.toString(vals[i]));
//                if ( (++cnt) % 10 == 0 ) {
//                    buf.append("\n");
//                } else {
//                    buf.append(" ");
//                }
//            }
//            logger.info(methodName, null, id, "STATS: vals:\n", buf.toString());
//        }
//
//    }

    /**
     * Update scheduler internal job structure with updated data from arriving job state.
     */
    void jobUpdate(Object state, IDuccWork job)
    {
    	String methodName = "jobUpdate";
        IDuccSchedulingInfo si = job.getSchedulingInfo();

        DuccId jobid = job.getDuccId();
        IRmJob j = scheduler.getJob(jobid);
        if ( j == null ) {
            // this can happen right when the job is submitted, if we haven't yet called
            // the scheduler to deal with it.  just ignore, but take note.
            // logger.info(methodName, jobid, "**** Cannot find job to update! ****");
            return;
        } else {            
            int total_work     = toInt(si.getWorkItemsTotal(), scheduler.getDefaultNTasks());
            int completed_work = toInt(si.getWorkItemsCompleted(), 0)  + toInt(si.getWorkItemsError(), 0);

            int max_shares     = toInt(si.getProcessesMax(), Integer.MAX_VALUE);
            int existing_max_shares = j.getMaxShares();

            int remaining_work = Math.max(total_work - completed_work, 0);

            double arith_mean = Double.NaN;
            IDuccPerWorkItemStatistics stats = si.getPerWorkItemStatistics();        
            if(stats != null) {
            	arith_mean = stats.getMean();
            }

            // The skewed mean is the arithmetic mean of work items both completed 
            // (if any) and active (if any).  All completed work items contribute,
            // but only active work items whose time already exceeds the mean of
            // the completed ones contribute.
            
            // To schedule, we always use the skewed_mean when it is > 0.
            
            double skewed_mean = si.getAvgTimeForWorkItemsSkewedByActive();
            
            logger.info(methodName, job.getDuccId(), 
                        String.format("tot: %d %s -> %s compl: %s err: %s rem: %d mean: %f skew: %f",
                                      total_work,  
                                      state,
                                      job.getStateObject(),
                                      si.getWorkItemsCompleted(),    // note this comes in as string (!) from OR
                                      si.getWorkItemsError(),        // also string
                                      remaining_work,
                                      arith_mean,
                                      skewed_mean
                                      ));

            if(skewed_mean > 0) {
            	arith_mean = skewed_mean;
            }
            
            if ( max_shares != existing_max_shares ) {
                j.setMaxShares(max_shares);
                logger.info(methodName, job.getDuccId(), "Max shares adjusted from", existing_max_shares, "to", max_shares, "(incoming)",
                            si.getProcessesMax());
            } 
                
            j.setNQuestions(total_work, remaining_work, arith_mean);

            // formatSchedulingInfo(job.getDuccId(), si, remaining_work);
            if ( job instanceof IDuccWorkJob ) {
                if ( j.setInitWait( ((IDuccWorkJob) job).isRunnable()) ) {
                    logger.info(methodName, jobid, "Set Initialized.");
                    scheduler.signalInitialized(j);
                }
                // UIMA-4275 Avoid race so we don't keep trying to give out new processes
                if ( ((IDuccWorkJob) job).isCompleting() ) {
                    j.markComplete();
                }
            } else {
                j.setInitWait(true);                           // pop is always ready to go
            }            
        }
    }

    /**
     * NOTE: If this returns false, it maust also refuse().
     */
    private boolean receiveExecutable(IRmJob j, IDuccWork job, boolean mustRecover)    // UIMA-4142, add mustRecover flag
    {
    	String methodName = "receiveExecutable";
        IDuccWorkExecutable de = (IDuccWorkExecutable) job;
        IDuccProcessMap     pm = de.getProcessMap();

        if ( mustRecover ) {                                   // need to recover
            for ( IDuccProcess proc : pm.values() ) {          // build up Shares from the incoming state

                ProcessState state = proc.getProcessState();                
                String pid = proc.getPID();                        
                NodeIdentity ni = proc.getNodeIdentity();

                if ( proc.isComplete() ) {
                    logger.debug(methodName, j.getId(), "Skipping process", pid, "on", ni.getCanonicalName(), "beacause state is", state);
                    continue;
                 }

                Machine m = scheduler.getMachine(ni);
                if ( m == null ) {                             // not known, huh? maybe next epoch it will have checked in
                    refuse(j, "Cannot restore job because node " + ni.getCanonicalName()  + " is unknown.");
                    return false;                              // so we don't add it to global tables
                } else {
                    DuccId id = proc.getDuccId();
                    Share   s = new Share(id, m, j, m.getShareOrder());               // guess share order; scheduler will reset when it recovers job
                                                                                      // UIMA-4275 include depth
                    long mem = proc.getResidentMemory();
                    long investment = proc.getWiMillisInvestment();
                    logger.info(methodName, j.getId(), "Assigning share in state", state, "pid", pid, "for recovery", s.toString());
                    j.recoverShare(s);
                    s.update(j.getId(), mem, investment, state, proc.getTimeWindowInit(), pid);                    
                }
            }
            logger.info(methodName, j.getId(), "Scheduling for recovery.");
            scheduler.signalRecovery(j);
        } else {
            logger.info(methodName, j.getId(), "Scheduling as new.");
            scheduler.signalNewWork(j);
        }
        return true;
    }

    /**
     * NOTE: If this returns false, it maust also refuse().
     */
    private boolean receiveReservation(IRmJob j, IDuccWork job, boolean mustRecover)  // UIMA-4142, add mustRecover flag
    {
    	String methodName = "receiveReservation";
        j.setReservation();

        IDuccWorkReservation dr = (IDuccWorkReservation) job;
        IDuccReservationMap rm = dr.getReservationMap();
        if ( mustRecover ) {                                   // need to recover
            for ( IDuccReservation res : rm.values() ) {       // build up Shares from the incoming state
                NodeIdentity ni = res.getNodeIdentity();
                Machine m = scheduler.getMachine(ni);
                if ( m == null ) {                             // not known, huh? maybe next epoch it will have checked in
                    refuse(j, "Cannot restore reservation because node " + ni.getCanonicalName() + " is unknown.");
                    return false;                              // so we don't add it to global tables
                } else {
                    DuccId id = res.getDuccId();
                    Share   s = new Share(id, m, j, m.getShareOrder());
                    s.setFixed();
                    j.recoverShare(s);
                    logger.debug(methodName, j.getId(), "Assigning share for recovery", s.toString());
                }
            }
            logger.info(methodName, j.getId(), "Scheduling for recovery.");
            scheduler.signalRecovery(j);
        } else {
            logger.info(methodName, j.getId(), "Scheduling as new.");
            scheduler.signalNewWork(j);
        }
        return true;
    }
   
    /**
     * Convert a JobManager Job into a ResourceManager RmJob.  We assume this job is NOT in
     * our lists.
     *
     * @param job
     */
    boolean jobArrives(IDuccWork job)
    {
    	String methodName = "jobArrives";
        logger.trace(methodName, job.getDuccId(), "Job arives");
        logger.trace(methodName, job.getDuccId(), "Job is of type", job.getDuccType());

        // Properties props = new Properties();
        
        // Set<String> keys = props.stringPropertyNames();
        // for ( String k : keys ) {
        //     logger.debug("methodName", job.getDuccId(), "Next property [", k, ", ", props.getProperty(k), "]");
        // }
        
        // Properties rmProps = new DuccProperties();
        // for ( int i = 0; i < requiredProperties.length; i++ ) {
        //     String v = props.getProperty(requiredProperties[i]);
        //     if ( v == null ) {
        //         v = defaultValues[i];
        //     }
        //     rmProps.setProperty(rmProperties[i], v);
        // }
        // IRmJob j = new RmJob(job.getDuccId(), rmProps);

        // Convert Lou's structure into mine.
        IRmJob j = new RmJob(job.getDuccId());

        boolean mustRecover = isRecovered(job);             // UIMA-4142

        IDuccSchedulingInfo si = job.getSchedulingInfo();
        IDuccStandardInfo   sti = job.getStandardInfo();
        
        String name       = sti.getDescription();
        if ( name == null ) {
            name = "A Job With No Name.";
        }
        String user_name  = sti.getUser().trim();
        j.setUserName(user_name);
        j.setJobName(name);
        j.setServiceId(toLong(job.getServiceId(), 0L)); // UIMA-4712 only non-zero on actual service instances 

        int threads       = toInt(si.getThreadsPerProcess(), scheduler.getDefaultNThreads());
        int user_priority = toInt(si.getSchedulingPriority(), 100);

        int total_work    =  toInt(si.getWorkItemsTotal(), scheduler.getDefaultNTasks());
        int completed_work = toInt(si.getWorkItemsCompleted(), 0);
        int remaining_work = Math.max(total_work - completed_work, 1);  // never let this go 0 or negative - both cases
                                                                        // are (probably user) errors.

        logger.info(methodName, job.getDuccId(), "total_work", total_work, "completed_work", completed_work,"remaining_work", remaining_work);

        int memory        = toInt(si.getMemorySizeRequested(), scheduler.getDefaultMemory());
        String className  = si.getSchedulingClass();
        if ( className == null ) {
            switch ( job.getDuccType() ) {
               case Job:              
                   className = scheduler.getDefaultFairShareName();
                   break;
               case Service:
               case Pop:
               case Reservation:
                   className = scheduler.getDefaultReserveName();
                   break;
            }
            if ( className == null ) {
                j.refuse("No scheduling class defined and no default class configured.");
                return false;
            }
        }

        j.setThreads(threads);
        j.setUserPriority(user_priority);
        j.setNQuestions(total_work, remaining_work, 0.0);
        j.setClassName(className);

        switch (si.getMemoryUnits()) {
            case GB:
                break;
            default:
                logger.warn(methodName, job.getDuccId(), "Memory units other than GB are not currently supported.  Job returned.");
                break;
        }
        j.setMemory(memory);
        j.init();

        j.setTimestamp(Long.parseLong(sti.getDateOfSubmission()));
        // logger.info(methodName, j.getId(), "SUBMISSION DATE:", subd, (new Date(subd)).toString());

        if ( job instanceof IDuccWorkJob ) {
            j.setInitWait( ((IDuccWorkJob) job).isRunnable());
        } else {
            j.setInitWait(true);                          // pop is always ready to go
        }

        j.setDuccType(job.getDuccType());                 // ugly and artificial but ... not going to rant here
                                                          // it's needed so messages can be made legible
        switch ( job.getDuccType() ) {                    // UIMA-4142 to distinguish between service and AP 
            case Service:
            case Pop:
                if  ( ((IDuccWorkService)job).getServiceDeploymentType() == ServiceDeploymentType.other )  {
                    j.setArbitraryProcess();
                }
                break;
            default:
                break;                    
        }


        //
        // Now: must either create a new job, or recover one that we didn't know about, on the assumption that we
        // have just crashed and are recovering.
        //
        // Be SURE that if status is turned false for any reason, or if you exit early with false, that you
        // refuse() the job.
        //
        boolean status = true;        
        
        int max_processes = 0;
       	// int max_machines = 0;	
        ResourceClass rescl = scheduler.getResourceClass(className);

        if ( rescl == null ) {
            // oh darn, we can't continue past this point
            refuse(j, "Cannot find priority class " + className + " for job");
            
            // UIMA-4142
            // However, fs this is recovery and we get here, it's because somehow the class definition
            // got deleted.  In this case there might be resources assigned.  We must evict if possible.
            // All affected hosts must be blacklisted.  We need to remember all this so we can unblacklist them
            // if the resources ever become free.
            blacklist(job, memory);
            return false;
        }
        if ( !rescl.authorized(user_name) ) { 
            // UIMA-4275
            // if not recovering, and the class is not authorized, stop it dead here
            // if we are recovering, might no longer be authorized - the main scheduler will
            // deal with this as appropriate for the scheduling policy.
            refuse(j, "User '" + user_name + "' not authorized to use class '" + className + "'");
            if ( ! mustRecover ) {
                return false;
            }
        }

        j.setResourceClass(rescl);

//         if ( logger.isDebug() ) {
//             logger.debug(methodName, j.getId(),"sharesMax", si.getSharesMax());
//                        logger.debug(methodName, j.getId(),"getInstancesCount", si.getInstancesCount());
//                        logger.debug(methodName, j.getId(), "rescl.getMaxProcesses", rescl.getMaxProcesses());
//                        logger.debug(methodName, j.getId(), "rescl.getMaxMachines", rescl.getMaxMachines());
//         }

        switch ( job.getDuccType() ) {
          // UIMA-4275, must enforce max allocations as 1 for Service and Pop/
          case Service:
          case Pop:
              switch ( rescl.getPolicy() ) {
                  case FAIR_SHARE:
                      refuse(j, "Services and managed reservations are not allowed to be FAIR_SHARE");
                      break;
                      
                  case FIXED_SHARE:
                      j.setMaxShares(1);
                      break;
                      
                  case RESERVE:
                      j.setMaxShares(1);
                      break;
              }
              status = receiveExecutable(j, job, mustRecover); // UIMA-4142, add mustRecover flag
              logger.trace(methodName, j.getId(), "Serivce, or Pop arrives, accepted:", status);
              break;
          case Job:              
              // instance and share count are a function of the class
              max_processes    = toInt(si.getProcessesMax(), DEFAULT_PROCESSES);
              switch ( rescl.getPolicy() ) {
                  case FAIR_SHARE:
                      j.setMaxShares(max_processes);
                      break;
                      
                  case FIXED_SHARE:
                      j.setMaxShares(max_processes);
                      break;
                      
                  case RESERVE:
                      // max_machines   = toInt(si.getSharesMax(), DEFAULT_INSTANCES);
                      j.setMaxShares(max_processes);
                      break;
              }
              
              status = receiveExecutable(j, job, mustRecover); // UIMA-4142, add mustRecover flag
              logger.trace(methodName, j.getId(), "Job arrives, accepted:", status);
              break;
          case Reservation:
              // UIMA-4275. non-jobs restricted to exactly one allocation per request 
              j.setMaxShares(1);

              status = receiveReservation(j, job, mustRecover);  // UIMA-4142, add mustRecover flag
              logger.trace(methodName, j.getId(), "Reservation arrives, accepted:", status);
              break;
          default:
              refuse(j, "Unknown job type: " + job.getDuccType());
              status = false;
              break;
        }
        
//         logger.debug(methodName, j.getId(), "Max_processes:", max_processes);
//         logger.debug(methodName, j.getId(), "Max_machines:", max_machines);

        return status;
    }

    /**
     * Our records indicate that we know about this job but JM doesn't so we purge
     * it from the scheduler
     * @param job
     */
    void jobRemoved(DuccId id)
    {
    	String methodName = "jobRemoved";
        logger.trace(methodName, id, "Signalling removal");
        scheduler.signalCompletion(id);
        localMap.removeDuccWork(id);
        logger.trace(methodName, id, "Remove signalled");
    }

    public void reconcileProcesses(DuccId jobid, IDuccWork l, IDuccWork r)
    {
    	String methodName = "reconcileProcess";
        IDuccProcessMap lpm = ((IDuccWorkJob )l).getProcessMap();
        IDuccProcessMap rpm = ((IDuccWorkJob)r).getProcessMap();

        @SuppressWarnings("unchecked")
        DuccMapDifference<DuccId, IDuccProcess> diffmap = DuccCollectionUtils.difference(lpm, rpm);

        // new stuff in in the left side of the map
        Map<DuccId, IDuccProcess> lproc = diffmap.getLeft();
        
        for ( IDuccProcess p : lproc.values() ) {
            // look up share, update resident memory, process state, investment (eventually), maybe pid?
            // simply update the share with the information.  we pass in the jobid as a sanity check so
            // we can crash or at least complain loudly on mismatch.

            Share s = scheduler.getShare(p.getDuccId());

            if(s != null) {
            	long mem = p.getResidentMemory();
                long investment = p.getWiMillisInvestment();
                ProcessState state = p.getProcessState();
                String pid = p.getPID();

                logger.info(methodName, jobid, "New process ", s.toString(), mem, state, pid);
                if ( ! s.update(jobid, mem, investment, state, p.getTimeWindowInit(), pid) ) {
                    // TODO: probably change to just a warning and cancel the job - for now I want an attention-getter
                    throw new SchedulingException(jobid, "Process assignemnt arrives for share " + s.toString() +
                                                  " but jobid " + jobid + " does not match share " + s.getJob().getId());
                }
                //scheduler.signalGrowth(jobid, s);
                // sadly, the pid is almost always null here
                //logger.info(methodName, jobid, 
                //            "New process arrives for share", s.toString(), "PID", pid);
            }
            else {
            	logger.warn(methodName, jobid, p.getDuccId(), "share not found?");
            }
        }
            
        // gone stuff in in the right side of the map
        Map<DuccId, IDuccProcess> rproc = diffmap.getRight();
        for ( IDuccProcess p : rproc .values()) {
            // these processes are done.  look up the job and tell it process complete.
            Share s = scheduler.getShare(p.getDuccId());
            IRmJob j = scheduler.getJob(jobid);
            if ( j == null ) {
                throw new SchedulingException(jobid, "Process completion arrives for share " + s.toString() +
                                              " but job " + jobid + "cannot be found.");
            }

            switch ( l.getDuccType() ) {        // UIMA-4326, if not a jobjob, the job must not get reallocations
                case Job:
                    break;
                default:
                    j.markComplete();
            }

            scheduler.signalCompletion(j, s);
            logger.info(methodName, jobid, 
                         String.format("Process %5s", p.getPID()),
                         "Completion:", s.toString());
        }

        for( DuccMapValueDifference<IDuccProcess> pd: diffmap ) {
            IDuccProcess pl = pd.getLeft();
            IDuccProcess pr = pd.getRight();

            Share sl = scheduler.getShare(pl.getDuccId());
            Share sr = scheduler.getShare(pr.getDuccId());

            String shareL = ( sl == null ) ? "<none>" : sl.toString();
            String shareR = ( sr == null ) ? "<none>" : sr.toString();

            ITimeWindow initL = pl.getTimeWindowInit();
            ITimeWindow initR = pr.getTimeWindowInit();
            long init_timeL = (initL == null) ? 0 : initL.getElapsedMillis();
            long init_timeR = (initR == null) ? 0 : initR.getElapsedMillis();

            /** extreme debugging only*/
            if ( logger.isTrace() ) {
                logger.trace(methodName, jobid, 
                             "\n\tReconciling. incoming.(did, pid, mem, state, share, initTime, investment)", 
                             pl.getDuccId(),
                             pl.getPID(),
                             pl.getResidentMemory(),
                             pl.getProcessState(),
                             shareL,
                             init_timeL,
                             pl.getWiMillisInvestment(),
                             "\n\tReconciling. existing.(did, pid, mem, state, share, initTime, investment)", 
                             pr.getDuccId(),
                             pr.getPID(),
                             pr.getResidentMemory(),
                             pr.getProcessState(),
                             shareR,
                             init_timeR,
                             pr.getWiMillisInvestment()
                             );
            } else {
                if ( (pr.getPID() == null) && (pl.getPID() != null) ) {
                      logger.trace(methodName, jobid, 
                                String.format("Process %5s", pl.getPID()),
                                "PID assignement for share", shareL);
                }
                if ( pl.getProcessState() != pr.getProcessState() ) {
                    logger.info(methodName, jobid, 
                                String.format("Process %5s", pl.getPID()), shareL,
                                "State:", pr.getProcessState(), "->", pl.getProcessState(),
                                getElapsedTime(pr.getTimeWindowInit()), getElapsedTime(pr.getTimeWindowRun()));
                }
            }

            long mem = pl.getResidentMemory();
            long investment = pl.getWiMillisInvestment();
            ProcessState state = pl.getProcessState();
            String pid = pl.getPID();                        
            Share s = scheduler.getShare(pl.getDuccId());
            if ( pl.isActive() ) {
                
                if ( s == null ) {
                    // this can happen if a node dies and the share is purged so it's ok.
                    logger.warn(methodName, jobid, "Update for share from process", pl.getPID(), pl.getDuccId(), "but cannot find share.");
                    continue;
                }

                // UIMA-3856 Can't do anything or else OR bugs will lose the state :(
                // if ( s.isPurged() ) {
                //     IRmJob j = scheduler.getJob(jobid);
                //     scheduler.signalCompletion(j, s);
                //     logger.info(methodName, jobid, "Process", pl.getPID(), "marked complete because it is purged. State:", state);
                // }

                if ( ! s.update(jobid, mem, investment, state, pl.getTimeWindowInit(), pid) ) {
                    // TODO: probably change to just a warning and cancel the job - for now I want an attention-getter
                    throw new SchedulingException(jobid, "Process update arrives for share " + s.toString() +
                                                  " but jobid " + jobid + " does not match job in share " + s.getJob().getId());
                }
                // logger.debug(methodName, jobid, "Process update to process ", pid, "mem", mem, "state", state, "is assigned for share", s.toString());

            } else if ( pl.isComplete() ) {
                IRmJob j = scheduler.getJob(jobid);
                if ( s != null ) {              // in some final states the share is already gone, not an error (e.g. Stopped)
                    scheduler.signalCompletion(j, s);          // signal the **process** (not job) is complete
                    logger.info(methodName, jobid, "Process", pl.getPID(), " completed due to state", state);
                }

                switch ( l.getDuccType() ) {        // UIMA-4326, if not a jobjob, the job must not get reallocations
                    case Job:
                        break;
                    default:
                        j.markComplete();
                }
                
            } else {
                logger.info(methodName, jobid, "Process", pl.getPID(), "ignoring update because of state", state);
            }
                    
        }            

    }

    boolean first_or_state = true;
    public void eventArrives(IDuccWorkMap jobMap)
    {
    	String methodName = "eventArrives";

        if ( jobMap.size() == 0 ) {
            logger.debug(methodName, null, "No state from Orchestrator");
            return;
        }

        // The init file is read and configured ?
        if ( ! scheduler.isInitialized() ) return;   // handle race, OR pub comes in while RM is (re)configuring itself

        if ( scheduler.mustRecover() ) {             // UIMA-4142 reconfig happened. 
            // must do this independently of isInitialized() since reinit could happen fully between OR pubs
            localMap = new DuccWorkMap();            // as if RM had been booted
            lastJobManagerUpdate = new JobManagerUpdate();
            blacklistedResources.clear();            // UIMA-4142
            refusedJobs.clear();
            first_or_state = true;
        }

        if ( first_or_state ) {
            first_or_state = false;
            scheduler.setRecovery(false);

            if ( ! recoverFromOrchestrator(jobMap) ) {
                logger.info(methodName, null, "There are no active jobs in map so can't build up state. Waiting for init stability.");
                return;
            } 
            
            if ( recovery ) {
                logger.info(methodName, null, "Fast recovery is enabled: Recovered state from Orchestrator, starting scheduler.");
                scheduler.start();
            }
        }

        // scheduler is readied either by fast-recovery, or by init stability
        if ( !scheduler.ready() ) {
            logger.info(methodName, null, "Orchestrator event is discarded: scheduler is waiting for init stability or is paused for reconfig..");
            return;
        }

        @SuppressWarnings("unchecked")
		DuccMapDifference<DuccId, IDuccWork> diffmap = DuccCollectionUtils.difference(jobMap, localMap);        

        for ( Object o : jobMap.values() ) {
        	IDuccWork w = (IDuccWork) o;
            logger.trace(methodName, w.getDuccId(), "Arrives in JmStateEvent state =", w.getStateObject());
        }

        //
        // First handle new stuff
        //
        Map<DuccId, IDuccWork> jobs = diffmap.getLeft();
        for ( IDuccWork w : jobs.values() ) {

            if ( w.isSchedulable() ) {
                logger.info(methodName, w.getDuccId(), "Incoming, state = ", w.getStateObject());
                try {
                    if ( jobArrives(w) ) {                // if not ... something is fubar and we have to ignore it for now
                        localMap.addDuccWork(w);
                    } 
                } catch ( Exception e ) {
                    logger.error(methodName, w.getDuccId(), "Can't receive job because of exception", e);
                }
            } else {
                logger.info(methodName, w.getDuccId(), "Received non-schedulable job, state = ", w.getStateObject());
                whitelist(w);                          // UIMA-4142 if blacklisted, clear everything
            }
        }
        
        jobs = diffmap.getRight();
        for ( IDuccWork w :jobs.values() ) {
            logger.info(methodName, w.getDuccId(), "Gone");
            jobRemoved(w.getDuccId());
        }

        //
        // Stuff on the left is incoming.  Stuff on the right is already in my map.
        //
        for( DuccMapValueDifference<IDuccWork> jd: diffmap ) {
            IDuccWork r = jd.getRight();
            IDuccWork l = jd.getLeft();

            if ( ! l.isSchedulable() ) {
                logger.info(methodName, l.getDuccId(), "Removing unschedulable:", r.getStateObject(), "->", l.getStateObject());
                jobRemoved(r.getDuccId());
            } else {

                localMap.addDuccWork(l);           // still schedulable, and we already know about it, just sync the state

                scheduler.signalState(l.getDuccId(), l.getStateObject().toString());
                switch ( l.getDuccType() ) {
                  case Job:    
                      jobUpdate(r.getStateObject(), l);
                      reconcileProcesses(l.getDuccId(), l, r);
                      break;
                  case Service:
                  case Pop:
                      // This is really an AP and OR sets the state to running immediately although it isn't yet, so the
                      // information is incomplete.  We always have to reconcile.
                      if  ( ((IDuccWorkService)l).getServiceDeploymentType() == ServiceDeploymentType.other )  {
                          logger.info(methodName, l.getDuccId(), "[P] State: ", r.getStateObject(), "->", l.getStateObject());
                          reconcileProcesses(l.getDuccId(), l, r);
                      } else  if ( r.getStateObject() != l.getStateObject() ) {
                          // Service state does come int correctly
                          logger.info(methodName, l.getDuccId(), "[S] State: ", r.getStateObject(), "->", l.getStateObject());
                          reconcileProcesses(l.getDuccId(), l, r);
                      }
                      break;
                  case Reservation:
                      if ( r.getStateObject() != l.getStateObject() ) {
                          logger.info(methodName, l.getDuccId(), "[R] State: ", r.getStateObject(), "->", l.getStateObject());
                      }
                      // for the moment, these guys have nothing to reconcile.
                      break;
                  case Undefined:
                      throw new SchedulingException(l.getDuccId(), "Work arrives as type Undefined - should have been filtered out by now.");                      
                }
            }
           
        }

        logger.trace(methodName, null, "Done with JmStateDuccEvent with some jobs processed");

    }

    /**
     * This is an ugly kludge because we discovered OR isn't doing map diffs!  So in the case
     * of lost messagees, OR may not be able to discover that jobs actually have shares assigned.
     *
     * Here we look into the OR map, dig out the "work", and if the indicated share is not
     * there, forcibly add it to the expanded shares list.
     */
    Map<Share, Share> sanityCheckForOrchestrator(IRmJob j, Map<Share, Share> shares, Map<Share, Share>expanded)
    {
        String methodName = "sanityCheckForOrchestrator";
        IDuccWork w = localMap.findDuccWork(j.getId());

        if ( w == null ) return null;                  // deal with race - the job could have completed right as we are ready to
                                                       // publish, in which case it's gone from localMap
        if ( shares == null ) return null;             // no shares for whatever reason, we couldn't care less ...

        Map<Share, Share> ret = new HashMap<Share, Share>();
        switch ( w.getDuccType() ) {
            case Job:
            case Service:
                {
                    IDuccWorkExecutable de = (IDuccWorkExecutable) w;
                    IDuccProcessMap pm = de.getProcessMap();
                    
                    for ( Share s : shares.values() ) {
                        IDuccProcess p = pm.get(s.getId());
                        if ( p == null ) {
                            if ( (expanded == null) || (!expanded.containsKey(s)) ) {
                                logger.warn(methodName, j.getId(), "Redrive share assignment: ", s);
                                ret.put(s, s);
                            }
                        }
                    }
                }
                break;

            case Reservation:
                {
                    IDuccWorkReservation de = (IDuccWorkReservation) w;
                    IDuccReservationMap  rm = de.getReservationMap();
                    
                    for ( Share s : shares.values() ) {
                        IDuccReservation r = rm.get(s.getId());
                        if ( r == null ) {
                            if ( (expanded == null) || (!expanded.containsKey(s)) ) {
                                logger.warn(methodName, j.getId(), "Redrive share assignment:", s);
                                ret.put(s, s);
                            }
                        }
                    }
                }
                break;
        }
        return ret;
    }

    // No longer needed after UIMA-4275
    // boolean isPendingNonPreemptable(IRmJob j) 
    // {
    // 	String methodName = "isPendingNonPreemptable";
    //     // If fair share it definitely isn't any kind of preemptable
    //     if ( j.getResourceClass().getPolicy() == Policy.FAIR_SHARE) return false;

    //     // otherwise, if the shares it has allocated is < the number it wants, it is in fact
    //     // pending but not complete.
    //     logger.trace(methodName, j.getId(), "countNShares", j.countNShares(), "countInstances", j.getMaxShares(), "isComplete", j.isCompleted());

    //     if ( j.isCompleted() ) {
    //         return false;
    //     }

    //     // 2014-02-18 - countTotalAssignments is the total nodes this job ever got - we're not allowed to
    //     //              add more.  But if a node dies and the share is canceled, countNShares() CAN return 
    //     //              0, preventing this cutoff check from working, and the job looks "refused" when in
    //     //              fact it's just hungy.  Hence, the change from countNShares to countTotalAssignments. 
    //     //                
    //     //              Note: The NodePool code that detects dead nodes is responsible for removing dead shares
    //     //              from jobs and should not remove shares from reservations, but it can remove shares
    //     //              from non-preemptables that aren't reservations.        
    //     //              UIMA-3613 jrc
    //     //if ( j.countNShares() == j.countInstances() ) {
    //     if ( j.countTotalAssignments() == j.getMaxShares() ) {
    //         j.markComplete();                  // non-preemptable, remember it finally got it's max
    //         return false;
    //     }

    //     return (j.countNShares() < j.getMaxShares());
    // }

    /**
     * If no state has changed, we just resend that last one.
     */
    Map<DuccId, IRmJobState> previousJobState = new HashMap<DuccId, IRmJobState>();


    /**
     * Here's where we make a IRmStateEvent from the JobManagerUpdate so the caller can publish it.
     */    
    public RmStateDuccEvent createState(JobManagerUpdate jmu)
    {
        String methodName = "createState";
        //ArrayList<IRmJobState> rmJobState = null;
        Map<DuccId, IRmJobState> rmJobState = null;

        if ( jmu == null ) {                     // no changes
            rmJobState = previousJobState;
        } else {
            rmJobState = new HashMap<DuccId, IRmJobState>();

            // Must handle all jobs that ar refused here in JMC because nobody else knows about them
            Map<IRmJob, IRmJob> refused = new HashMap<IRmJob, IRmJob>();
            synchronized(refusedJobs) {
                refused.putAll(refusedJobs);
                refusedJobs.clear();
            }
            
            for ( IRmJob j : refused.values() ) {
                RmJobState rjs = new RmJobState(j.getId(), j.getRefusalReason());
                rjs.setDuccType(j.getDuccType());
                rmJobState.put(j.getId(), rjs);
            }

            // Now handle the jobs that made it into the scheduler proper
            Map<DuccId, IRmJob> jobs = jmu.getAllJobs();
            Map<DuccId, HashMap<Share, Share>> shrunken = jmu.getShrunkenShares();
            Map<DuccId, HashMap<Share, Share>> expanded = jmu.getExpandedShares();
//          for ( DuccId id : expanded.keySet() ) {
//              logger// .info(methodName, id, "Fetched these expanded shares:", expanded.get(id));
//          }

            /**
             * Convert RM internal state into the simplified externally published state.
             */
            for (IRmJob j : jobs.values()) {

                if ( j.isRefused() ) {
                    RmJobState rjs = new RmJobState(j.getId(), j.getRefusalReason());
                    rjs.setDuccType(j.getDuccType());
                    rmJobState.put(j.getId(), rjs);
                    jobRemoved(j.getId());
                    logger.warn(methodName, j.getId(), "Refusal: ", j.getRefusalReason());
                    continue;
                }

                Map<DuccId, IResource> all_shares      = new LinkedHashMap<DuccId, IResource>();
                Map<DuccId, IResource> shrunken_shares = new LinkedHashMap<DuccId, IResource>();
                Map<DuccId, IResource> expanded_shares = new LinkedHashMap<DuccId, IResource>();
                Map<Share, Share> shares = null;
                Map<Share, Share> redrive = null;

                shares = j.getAssignedShares();
                if ( shares != null ) {
                    ArrayList<Share> sorted = new ArrayList<Share>(shares.values());
                    Collections.sort(sorted, new RmJob.ShareByInvestmentSorter());
                    for ( Share s : sorted ) {
                        Resource r = new Resource(s.getId(), s.getNode(), s.isPurged(), s.getShareOrder(), s.getInitializationTime());
                        all_shares.put(s.getId(), r);
                    }
                    redrive = sanityCheckForOrchestrator(j, shares, expanded.get(j.getId()));
                }
                    
                shares = shrunken.get(j.getId());
                if ( shares != null ) {
                    for ( Share s : shares.values() ) {
                        Resource r = new Resource(s.getId(), s.getNode(), s.isPurged(), s.getShareOrder(), 0);
                        shrunken_shares.put(s.getId(), r);
                    }
                }                                        
                    
                shares = expanded.get(j.getId());
                if ( shares != null ) {                    
                    for ( Share s : shares.values() ) {
                        Resource r = new Resource(s.getId(), s.getNode(), s.isPurged(), s.getShareOrder(), 0);
                        expanded_shares.put(s.getId(), r);
                    }
                }
                    
                if ( redrive != null ) {
                    for ( Share s : redrive.values() ) {
                        Resource r = new Resource(s.getId(), s.getNode(), s.isPurged(), s.getShareOrder(), 0);
                        expanded_shares.put(s.getId(), r);
                    }
                }
                
                RmJobState rjs = new RmJobState(j.getId(), (j.getShareQuantum() >> 20) * (j.getShareOrder()), all_shares, shrunken_shares, expanded_shares);
                rjs.setDuccType(j.getDuccType());
                rjs.setReason(j.getReason());
                rmJobState.put(j.getId(), rjs);
            }

            // UIMA-4142 Add the blacklist to the mix
            rmJobState.putAll(blacklistedResources);
            blacklistedResources.clear();

            previousJobState = rmJobState;
        }        

        RmStateDuccEvent response = new RmStateDuccEvent(rmJobState);
        try {
            logger.info(methodName, null, "Schedule sent to Orchestrator");
            logger.info(methodName, null, response.toString() );            
        } catch (Exception e) {
            logger.error(methodName, null, e);
        }
        
        return response;

    }

    /**
     * Got an OR map and we're ok for fast recovery.  If the map has no "live" jobs we just ignore it - that's first-time
     * startup and OR will not start if there is no JD node, so we do normal init stability.  Otherwise, we assume that the
     * JD node is included, build the resource map, and allow scheduling to proceed.
     */
    boolean recoverFromOrchestrator(IDuccWorkMap jobmap)
    {
    	String methodName = "recoverFromOrchestrator";
        Map<Node, Node> nodes = new HashMap<Node, Node>();
        for ( Object o : jobmap.values() ) {
        	IDuccWork w = (IDuccWork) o;
        	String prefix = "?";
            switch ( w.getDuccType() ) {
            case Job:
                prefix = "JOB";
                break;
            case Service:
                prefix = "SVC";
                break;
            case Reservation:
                prefix = "RES";
                break;
            }

            if ( w.isCompleted() ) {
                logger.info(methodName, w.getDuccId(), "Ignoring completed work:", w.getDuccType(), ":", w.getStateObject());
                continue;
            }
                        		
            switch ( w.getDuccType() ) {
                case Job:
                case Service:
                    {
                        IDuccWorkExecutable de = (IDuccWorkExecutable) w;
                        IDuccProcessMap pm = de.getProcessMap();
                        logger.info(methodName, w.getDuccId(), "Receive:", prefix, w.getDuccType(), w.getStateObject(), "processes[", pm.size(), "] Completed:", w.isCompleted());

                        for ( IDuccProcess proc : pm.values() ) {
                            String pid = proc.getPID();
                            ProcessState state = proc.getProcessState();
                            Node n = proc.getNode();
                            if ( n == null ) {
                                logger.info(methodName, w.getDuccId(), "   Process[", pid, "] state [", state, "] is complete[", proc.isComplete(), "] Node [N/A] mem[N/A]");
                            }
                            else if( proc.isComplete() ) {
                            	long mem = n .getNodeMetrics().getNodeMemory().getMemTotal();
                            	logger.info(methodName, w.getDuccId(), "   Process[", pid, "] state [", state, "] is complete[", proc.isComplete(), "] Node [",n.getNodeIdentity().getCanonicalName() + "." + proc.getDuccId(),"] mem[", mem, "]");
                            }
                            else {
                                long mem = n .getNodeMetrics().getNodeMemory().getMemTotal();
                                logger.info(methodName, w.getDuccId(), 
                                            "   Process[", pid, 
                                            "] state [", state, 
                                            "] is complete [", proc.isComplete(),
                                            "] Node [", n.getNodeIdentity().getCanonicalName() + "." + proc.getDuccId(),                                            
                                            "] mem [", mem, "]");                    
                                logger.info(methodName, w.getDuccId(), "      Recover node[", n.getNodeIdentity().getCanonicalName());
                                // 
                                // Note, not ignoring dead processes belonging to live jobs.  Is this best or should we be
                                // more conservative and not use nodes that we don't know 100% for sure are ok?
                                //
                                nodes.put(n, n);
                            }
                        }
                    }
                    break;

                // case Service: 
                //     {
                //         IDuccWorkExecutable de = (IDuccWorkExecutable) w;
                //         IDuccProcessMap pm = de.getProcessMap();
                //         logger.info(methodName, w.getDuccId(), prefix, w.getDuccType(), "processes[", pm.size(), "].");
                //     }
                //     break;
                    
                case Reservation: 
                    {
                        IDuccWorkReservation de = (IDuccWorkReservation) w;
                        IDuccReservationMap  rm = de.getReservationMap();

                        logger.info(methodName, w.getDuccId(), "Receive:", prefix, w.getDuccType(), w.getStateObject(), "processes[", rm.size(), "] Completed:", w.isCompleted());
                        
                        if(w.isCompleted()) {
                        	continue;
                        }
                        
                        for ( IDuccReservation r: rm.values()) {
                            Node n = r.getNode();                        
                            if ( n == null ) {
                                logger.info(methodName, w.getDuccId(), 
                                            "    Node [N/A] mem[N/A");
                            } else {
                                long mem = n .getNodeMetrics().getNodeMemory().getMemTotal();
                                logger.info(methodName, w.getDuccId(), 
                                            "   Node[", n.getNodeIdentity().getCanonicalName(),
                                            "] mem[", mem, "]");
                                nodes.put(n, n);
                            }
                        }
                    }
                    break;

                default:
                    logger.info(methodName, w.getDuccId(), "Received work of type ?", w.getDuccType());
                    break;
            }
        }
        logger.info(methodName, null, "Recovered[", nodes.size(), "] nodes from OR state.");
        for (Node n : nodes.values() ) {
            nodeStability.nodeArrives(n);
        }
        
        return (nodes.size() != 0);
    }

}

