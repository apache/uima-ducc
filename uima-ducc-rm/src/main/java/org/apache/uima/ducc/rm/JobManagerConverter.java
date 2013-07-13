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

import java.util.HashMap;
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
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
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
    
    DuccWorkMap localMap = null;
    JobManagerUpdate lastJobManagerUpdate = new JobManagerUpdate();

    Map<IRmJob, IRmJob> refusedJobs = new HashMap<IRmJob, IRmJob>();
    
    boolean recovery = false;

    public JobManagerConverter(ISchedulerMain scheduler)
    {
        this.scheduler = scheduler;
        this.localMap = new DuccWorkMap();
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
  
    void refuse(IRmJob j, String reason)
    {
        j.refuse(reason);
        synchronized(refusedJobs) {
            refusedJobs.put(j, j);
        }
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
    void jobUpdate(IDuccWork job)
    {
    	String methodName = "jobUpate";
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
            int completed_work = toInt(si.getWorkItemsCompleted(), 0)  + toInt(si.getWorkItemsError(), 0)+ toInt(si.getWorkItemsLost(), 0);

            int max_shares     = toInt(si.getSharesMax(), Integer.MAX_VALUE);
            int existing_max_shares = j.getMaxShares();

            // we need at least 1 if the job isn't reported complete, in case the user forgot to set the
            // work item count.  the job will run, but slowly in that case.
            int remaining_work = Math.max(total_work - completed_work, 1);

            logger.info(methodName, job.getDuccId(), 
                        String.format("total_work: %5d items completed: %5s items error %3s remaining work %5d",
                                      total_work,  
                                      si.getWorkItemsCompleted(),    // note this comes in as string (!) from OR
                                      si.getWorkItemsError(),        // also string
                                      remaining_work
                                      ));

            if ( max_shares != existing_max_shares ) {
                j.setMaxShares(max_shares);
                logger.info(methodName, job.getDuccId(), "Max shares adjusted from", existing_max_shares, "to", max_shares, "(incoming)",
                            si.getSharesMax());
            } 
                
            double arith_mean = Double.NaN;
            IDuccPerWorkItemStatistics stats = si.getPerWorkItemStatistics();        
            if(stats != null) {
            	arith_mean = stats.getMean();
            }
            j.setNQuestions(total_work, remaining_work, arith_mean);

            // formatSchedulingInfo(job.getDuccId(), si, remaining_work);

            if ( job instanceof IDuccWorkJob ) {
                if ( j.setInitWait( ((IDuccWorkJob) job).isRunnable()) ) {
                    logger.debug(methodName, jobid, "Set Initialized.");
                    scheduler.signalInitialized(j);
                }
            } else {
                j.setInitWait(true);                           // pop is always ready to go
            }            
        }
    }

    /**
     * NOTE: If this returns false, it maust also refuse().
     */
    private boolean receiveExecutable(IRmJob j, IDuccWork job)
    {
    	String methodName = "receiveExecutable";
        IDuccWorkExecutable de = (IDuccWorkExecutable) job;
        IDuccProcessMap     pm = de.getProcessMap();

        if ( (pm.size() > 0) && !job.isCompleted() ) {          // need to recover, apparently RM crashed. hmph.
            for ( IDuccProcess proc : pm.values() ) {          // build up Shares from the incoming state

                ProcessState state = proc.getProcessState();                
                String pid = proc.getPID();                        
                NodeIdentity ni = proc.getNodeIdentity();

                if ( proc.isComplete() ) {
                    logger.debug(methodName, j.getId(), "Skipping process", pid, "on", ni.getName(), "beacause state is", state);
                    continue;
                 }

                Machine m = scheduler.getMachine(ni);
                if ( m == null ) {                             // not known, huh? maybe next epoch it will have checked in
                    refuse(j, "Cannot restore job because node " + ni.getName()  + " is unknown.");
                    return false;                              // so we don't add it to global tables
                } else {
                    DuccId id = proc.getDuccId();
                    Share   s = new Share(id, m, j, m.getShareOrder());        // guess share order; scheduler will reset when it recovers job
                    long mem = proc.getResidentMemory();

                    logger.info(methodName, j.getId(), "Assigning share in state", state, "pid", pid, "for recovery", s.toString());
                    j.recoverShare(s);
                    s.update(j.getId(), mem, state, proc.getTimeWindowInit(), proc.getTimeWindowRun(), pid);                    
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
    private boolean receiveReservation(IRmJob j, IDuccWork job)
    {
    	String methodName = "receiveReservation";
        j.setReservation();

        IDuccWorkReservation dr = (IDuccWorkReservation) job;
        IDuccReservationMap rm = dr.getReservationMap();
        if ( (rm.size() > 0) && !job.isCompleted() ) {          // need to recover, apparently RM crashed. hmph.
            for ( IDuccReservation res : rm.values() ) {       // build up Shares from the incoming state
                NodeIdentity ni = res.getNodeIdentity();
                Machine m = scheduler.getMachine(ni);
                if ( m == null ) {                             // not known, huh? maybe next epoch it will have checked in
                    refuse(j, "Cannot restore reservation because node " + ni.getName() + " is unknown.");
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
     * NOTE IMPORTANT NOTE
     *
     *    Until Lou's job contains all required scheduling fields I do a conversion that enhances
     *    what I receive with defaults so the scheduler can actually schedule the job.
     *
     * NOTE IMPORTANT NOTE
     *
     * @param job
     */
    boolean jobArrives(IDuccWork job)
    {
    	String methodName = "jobArrives";
        logger.debug(methodName, job.getDuccId(), "Job arives");
        logger.debug(methodName, job.getDuccId(), "Job is of type", job.getDuccType());

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
        
        IDuccSchedulingInfo si = job.getSchedulingInfo();
        IDuccStandardInfo   sti = job.getStandardInfo();
        
        String name       = sti.getDescription();
        String user_name  = sti.getUser();
        j.setUserName(user_name);
        j.setJobName(name);

        int min_shares    = toInt(si.getSharesMin(), 0);
        int threads       = toInt(si.getThreadsPerShare(), scheduler.getDefaultNThreads());
        int user_priority = toInt(si.getSchedulingPriority(), 100);

        int total_work    =  toInt(si.getWorkItemsTotal(), scheduler.getDefaultNTasks());
        int completed_work = toInt(si.getWorkItemsCompleted(), 0);
        int remaining_work = Math.max(total_work - completed_work, 1);  // never let this go 0 or negative - both cases
                                                                        // are (probably user) errors.

        logger.info(methodName, job.getDuccId(), "total_work", total_work, "completed_work", completed_work,"remaining_work", remaining_work);

        int memory        = toInt(si.getShareMemorySize(), scheduler.getDefaultMemory());
        String className  = si.getSchedulingClass();
        if ( className == null ) {
            className = scheduler.getDefaultClassName();
        }

        j.setMinShares(min_shares);
        j.setThreads(threads);
        j.setUserPriority(user_priority);
        j.setNQuestions(total_work, remaining_work, 0.0);
        j.setClassName(className);

        switch (si.getShareMemoryUnits()) {
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

        //
        // Now: must either create a new job, or recover one that we didn't know about, on the assumption that we
        // have just crashed and are recovering.
        //
        // Be SURE that if status is turned false for any reason, or if you exit early with false, that you
        // refuse() the job.
        //
        boolean status = true;        
        
        int max_processes = 0;
       	int max_machines = 0;	
        ResourceClass rescl = scheduler.getResourceClass(className);

        if ( rescl == null ) {
            // ph darn, we can't continue past this point
            refuse(j, "Cannot find priority class " + className + " for job");
            return false;
        }

//         if ( logger.isDebug() ) {
//             logger.debug(methodName, j.getId(),"sharesMax", si.getSharesMax());
//                        logger.debug(methodName, j.getId(),"getInstancesCount", si.getInstancesCount());
//                        logger.debug(methodName, j.getId(), "rescl.getMaxProcesses", rescl.getMaxProcesses());
//                        logger.debug(methodName, j.getId(), "rescl.getMaxMachines", rescl.getMaxMachines());
//         }

        switch ( job.getDuccType() ) {
          case Service:
          case Pop:
          case Job:              
              // instance and share count are a function of the class
              switch ( rescl.getPolicy() ) {
                  case FAIR_SHARE:
                      max_processes    = toInt(si.getSharesMax(), DEFAULT_PROCESSES);
                      max_processes    = Math.min(rescl.getMaxProcesses(), max_processes);
                      j.setMaxShares(max_processes);
                      j.setNInstances(-1);
                      break;
                      
                  case FIXED_SHARE:
                      max_processes   = toInt(si.getSharesMax(), DEFAULT_INSTANCES);
                      j.setMaxShares(max_processes);
                      j.setNInstances(max_processes);
                      break;
                      
                  case RESERVE:
                      max_machines   = toInt(si.getSharesMax(), DEFAULT_INSTANCES);
                      j.setMaxShares(max_machines);
                      j.setNInstances(max_machines);
                      break;
              }
              
              status = receiveExecutable(j, job);
              logger.debug(methodName, j.getId(), "Serivce, Pop, or Job arrives, accepted:", status);
              break;
          case Reservation:
              switch ( rescl.getPolicy() ) {
                  case FIXED_SHARE:
                      max_machines   = toInt(si.getInstancesCount(), DEFAULT_INSTANCES);
                      break;
                  case RESERVE:
                      max_machines   = toInt(si.getInstancesCount(), DEFAULT_INSTANCES);
                      break;
              }
                            
              j.setMaxShares(-1);
              j.setNInstances(max_machines);

              status = receiveReservation(j, job);
              logger.debug(methodName, j.getId(), "Reservation arrives, accepted:", status);
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
        logger.debug(methodName, id, "Signalling removal");
        scheduler.signalCompletion(id);
        localMap.removeDuccWork(id);
        logger.debug(methodName, id, "Remove signalled");
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
            long mem = p.getResidentMemory();
            ProcessState state = p.getProcessState();
            String pid = p.getPID();
            if ( ! s.update(jobid, mem, state, p.getTimeWindowInit(), p.getTimeWindowRun(), pid) ) {
                // TODO: probably change to just a warning and cancel the job - for now I want an attention-getter
                throw new SchedulingException(jobid, "Process assignemnt arrives for share " + s.toString() +
                                              " but jobid " + jobid + " does not match share " + s.getJob().getId());
            }
            //scheduler.signalGrowth(jobid, s);
            // sadly, the pid is almost always null here
            //logger.info(methodName, jobid, 
            //            "New process arrives for share", s.toString(), "PID", pid);
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
            scheduler.signalCompletion(j, s);
            logger.info(methodName, jobid, 
                         String.format("Process %5s", p.getPID()),
                         "Completes in share", s.toString());
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
                             "\n\tReconciling. incoming.(pid, mem, state, share, initTime)", 
                             pl.getPID(),
                             pl.getResidentMemory(),
                             pl.getProcessState(),
                             shareL,
                             init_timeL,
                             "\n\tReconciling. existing.(pid, mem, state, share, initTime)", 
                             pr.getPID(),
                             pr.getResidentMemory(),
                             pr.getProcessState(),
                             shareR,
                             init_timeR
                             );
            } else {
                if ( (pr.getPID() == null) && (pl.getPID() != null) ) {
                    logger.info(methodName, jobid, 
                                String.format("Process %5s", pl.getPID()),
                                "PID assignement for share", shareL);
                }
                if ( pl.getProcessState() != pr.getProcessState() ) {
                    logger.info(methodName, jobid, 
                                String.format("Process %5s", pl.getPID()),
                                "State update:", pr.getProcessState(), "-->", pl.getProcessState());
                }
            }

            long mem = pl.getResidentMemory();
            ProcessState state = pl.getProcessState();
            String pid = pl.getPID();                        
            Share s = scheduler.getShare(pl.getDuccId());
            if ( pl.isActive() ) {
                
                if ( s == null ) {
                    // this can happen if a node dies and the share is purged so it's ok.
                    logger.warn(methodName, jobid, "Update for share from process", pl.getPID(), pl.getDuccId(), "but cannot find share.");
                    continue;
                }
                
//                 if ( s.isPurged() ) {
//                     IRmJob j = scheduler.getJob(jobid);
//                     scheduler.signalCompletion(j, s);
//                     logger.info(methodName, jobid, "Process", pl.getPID(), "marked complete because it is purged. State:", state);
//                 }

                if ( ! s.update(jobid, mem, state, pl.getTimeWindowInit(), pl.getTimeWindowRun(), pid) ) {
                    // TODO: probably change to just a warning and cancel the job - for now I want an attention-getter
                    throw new SchedulingException(jobid, "Process update arrives for share " + s.toString() +
                                                  " but jobid " + jobid + " does not match job in share " + s.getJob().getId());
                }
                // logger.debug(methodName, jobid, "Process update to process ", pid, "mem", mem, "state", state, "is assigned for share", s.toString());

            } else if ( pl.isComplete() ) {
                if ( s != null ) {              // in some final states the share is already gone, not an error (e.g. Stopped)
                    IRmJob j = scheduler.getJob(jobid);
                    scheduler.signalCompletion(j, s);
                    logger.debug(methodName, jobid, "Process", pl.getPID(), " completed due to state", state);
                }
            } else {
                logger.debug(methodName, jobid, "Process", pl.getPID(), "ignoring update because of state", state);
            }
                    
        }            

    }

    boolean first_or_state = true;
    public void eventArrives(DuccWorkMap jobMap)
    {
    	String methodName = "eventArrives";

        logger.debug(methodName, null, "Got Orchestrator");

        if ( jobMap.size() == 0 ) {
            logger.debug(methodName, null, "No state from Orchestrator");
            return;
        }

        // The init file is read and configured ?
        if ( ! scheduler.isInitialized() ) return;
        
        if ( first_or_state ) {
            first_or_state = false;
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
            logger.info(methodName, null, "Orchestrator event is discarded: waiting for init stability.");
            return;
        }

        @SuppressWarnings("unchecked")
		DuccMapDifference<DuccId, IDuccWork> diffmap = DuccCollectionUtils.difference(jobMap, localMap);        

        for ( IDuccWork w : jobMap.values() ) {
        	//IDuccWork j = (IDuccWork) w;
            logger.debug(methodName, w.getDuccId(), "Arrives in JmStateEvent state =", w.getStateObject());
        }

        //
        // First handle new stuff
        //
        Map<DuccId, IDuccWork> jobs = diffmap.getLeft();
        for ( IDuccWork w : jobs.values() ) {

            if ( w.isSchedulable() ) {
                logger.debug(methodName, w.getDuccId(), "Incoming, state = ", w.getStateObject());
                try {
                    if ( jobArrives(w) ) {                // if not ... something is fubar and we have to ignore it for now
                        localMap.addDuccWork(w);
                    } 
                } catch ( Exception e ) {
                    logger.error(methodName, w.getDuccId(), "Can't receive job because of exception", e);
                }
            } else {
                logger.debug(methodName, w.getDuccId(), "Received non-schedulable job, state = ", w.getStateObject());
            }
        }
        
        jobs = diffmap.getRight();
        for ( IDuccWork w :jobs.values() ) {
            logger.debug(methodName, w.getDuccId(), "Gone");
            jobRemoved(w.getDuccId());
        }

        // Todo : manage stuff that is diffs, not new, not deletions
        for( DuccMapValueDifference<IDuccWork> jd: diffmap ) {
            IDuccWork r = jd.getRight();
            IDuccWork l = jd.getLeft();
            logger.debug(methodName, l.getDuccId(), "Reconciling, incoming state = ", l.getStateObject(), " my state = ", r.getStateObject());

            if ( ! l.isSchedulable() ) {
                logger.debug(methodName, l.getDuccId(), "Removing unschedulable job state = ", r.getStateObject());
                jobRemoved(r.getDuccId());
            } else {

                localMap.addDuccWork(l);           // still schedulable, and we already know about it, just sync the state

                // 
                // Terrible ugliness here because the states should be so hard to get a handle on.
                //
                // Nonetheless, if this code hits, it's because the same job keeps coming back in
                // WaitingForResources so it's not in the left-side map.  We need to insure the
                // scheduler runs an epoch next opportunity it has.
                //
                // NO LONGER NEEDED because scheduler always runs without checking if there's work to do.
                //
                // Object s = l.getStateObject();
                // if ( s instanceof JobState ) {
                //     if ( ((JobState) s)  == JobState.WaitingForResources ) {
                //         logger.debug(methodName, l.getDuccId(), "Job: force epoch");
                //         scheduler.signalForceEpoch();
                //         continue;
                //     }
                //   }

                // if ( s instanceof ReservationState ) {
                //     if ( ((ReservationState) s)  == ReservationState.WaitingForResources ) {
                //         logger.debug(methodName, l.getDuccId(), "Reservation: force epoch");
                //         scheduler.signalForceEpoch();
                //         continue;
                //     }
                //   }


                switch ( l.getDuccType() ) {
                  case Job:    
                      jobUpdate(l);
                      reconcileProcesses(l.getDuccId(), l, r);
                      break;
                  case Service:
                  case Pop:
                  case Reservation:
                      // for the moment, these guyes have nothing to reconcile.
                      break;
                  case Undefined:
                      throw new SchedulingException(l.getDuccId(), "Work arrives as type Undefined - should have been filtered out by now.");                      
                }
            }
           
        }

        logger.debug(methodName, null, "Done with JmStateDuccEvent with some jobs processed");

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

        Map<Share, Share> ret = new HashMap<Share, Share>();
        if ( shares == null ) return null;                                    // no shares for whatever reason, we couldn't care less ...

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
            logger.debug(methodName, null, "Publishing old RM state");
            rmJobState = previousJobState;
        } else {
            // TODO: create a new rmJobState; remember to set previousJobState
            logger.debug(methodName, null, "Publishing new RM state");

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

                Map<DuccId, IResource> all_shares      = new HashMap<DuccId, IResource>();
                Map<DuccId, IResource> shrunken_shares = new HashMap<DuccId, IResource>();
                Map<DuccId, IResource> expanded_shares = new HashMap<DuccId, IResource>();
                Map<Share, Share> shares = null;
                Map<Share, Share> redrive = null;
                
                shares = j.getAssignedShares();
                if ( shares != null ) {
                    for ( Share s : shares.values() ) {
                        Resource r = new Resource(s.getId(), s.getNode(), s.isPurged(), s.getShareOrder());
                        all_shares.put(s.getId(), r);
                    }
                    redrive = sanityCheckForOrchestrator(j, shares, expanded.get(j.getId()));
                }

                shares = shrunken.get(j.getId());
                if ( shares != null ) {
                    for ( Share s : shares.values() ) {
                        Resource r = new Resource(s.getId(), s.getNode(), s.isPurged(), s.getShareOrder());
                        shrunken_shares.put(s.getId(), r);
                    }
                }

                shares = expanded.get(j.getId());
                if ( shares != null ) {
                    for ( Share s : shares.values() ) {
                        Resource r = new Resource(s.getId(), s.getNode(), s.isPurged(), s.getShareOrder());
                        expanded_shares.put(s.getId(), r);
                    }
                }

                if ( redrive != null ) {
                    for ( Share s : redrive.values() ) {
                        Resource r = new Resource(s.getId(), s.getNode(), s.isPurged(), s.getShareOrder());
                        expanded_shares.put(s.getId(), r);
                    }
                }

                RmJobState rjs = new RmJobState(j.getId(), all_shares, shrunken_shares, expanded_shares);
                rjs.setDuccType(j.getDuccType());
                rmJobState.put(j.getId(), rjs);
            }

            previousJobState = rmJobState;
        }        

        RmStateDuccEvent response = new RmStateDuccEvent(rmJobState);
        try {
            logger.debug(methodName, null, response.toString() );            
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
    boolean recoverFromOrchestrator(DuccWorkMap jobmap)
    {
    	String methodName = "recoverFromOrchestrator";
        Map<Node, Node> nodes = new HashMap<Node, Node>();
        for ( IDuccWork w : jobmap.values() ) {
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
                                logger.info(methodName, w.getDuccId(), "   Process[", pid, "] state [", state, "] is complete[", proc.isComplete(), "] Node [N/A] mem[N/A");
                            } else {
                                long mem = n .getNodeMetrics().getNodeMemory().getMemTotal();
                                logger.info(methodName, w.getDuccId(), 
                                            "   Process[", pid, 
                                            "] state [", state, 
                                            "] is complete [", proc.isComplete(),
                                            "] Node [", n.getNodeIdentity().getName() + "." + proc.getDuccId(),                                            
                                            "] mem [", mem, "]");                    
                                logger.info(methodName, w.getDuccId(), "      Recover node[", n.getNodeIdentity().getName());
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
                        
                        for ( IDuccReservation r: rm.values()) {
                            Node n = r.getNode();                        
                            if ( n == null ) {
                                logger.info(methodName, w.getDuccId(), 
                                            "    Node [N/A] mem[N/A");
                            } else {
                                long mem = n .getNodeMetrics().getNodeMemory().getMemTotal();
                                logger.info(methodName, w.getDuccId(), 
                                            "   Node[", n.getNodeIdentity().getName(),
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
            scheduler.nodeArrives(n);
        }
        
        return (nodes.size() != 0);
    }
}

