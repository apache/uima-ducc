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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

public class Experiment implements IExperiment {
	
	private String user = null;
	private String directory = null;
	private ArrayList<Task> tasks = null;
	private long fileDate = 0;
	
	private String id = UUID.randomUUID().toString();
    private int version;
	
	public Experiment(String user, String directory, long date, int version, ArrayList<Task> tasks) throws Exception {
		initialize(user, directory, date, version, tasks);
	}
	
	private void initialize(String user, String directory, long fileDate, int version, ArrayList<Task> tasks) throws Exception {
		if(user == null) {
			throw new ExperimentException("missing user");
		}
		if(directory == null) {
			throw new ExperimentException("missing directory");
		}
		if(tasks == null) {
			throw new ExperimentException("missing tasks");
		}
		this.user = user;
		this.directory = directory;
		this.fileDate = fileDate;
		this.version = version;
		this.tasks = tasks;
	}
	
	@Override
	public void setId(String value) {
		id = value;
	}
	
	@Override
	public String getId() {
		return id;
	}

	@Override
	public String getUser() {
		return user;
	}

	@Override
	public String getDirectory() {
		return directory;
	}
	
	@Override
	public ArrayList<Task> getTasks() {
		return tasks;
	}
	
	@Override
	public ArrayList<String> getJobIds() {
		ArrayList<String> jobIds = new ArrayList<String>();
		if(tasks != null) {
			for(Task task : tasks) {
				if(task.type != null) {
					Jed.Type jedType = Jed.Type.getEnum(task.type);
					switch(jedType) {
					case DuccJob:
					case Java:
					case Trainer:	
						long[] duccIdList = task.duccId;
						for(long duccId : duccIdList) {
							if(duccId < 0) {
								// reservation
							}
							else {
								// job
								String jobId = ""+(0+duccId);
								jobIds.add(jobId);
							}
						}
						break;
					default:
						break;
					}
				}
			}
		}
		return jobIds;
	}
	
    @Override
    public int getVersion() {
        return version;
    }
  
  @Override
  public boolean isActive() {
    boolean retVal = false;
    switch (getStatus()) {
      case Running:
        retVal = true;
        break;
      default:
        break;
    }
    return retVal;
  }
	
	@Override
	public Jed.Status getStatus() {
		Jed.Status retVal = Jed.Status.Unknown;
		ArrayList<Task> tasks = getTasks();
		if(tasks != null) {
			boolean canceled = false;
			boolean failed = false;
			boolean running = false;
			boolean done = false;
            for (Task task : tasks) {
                if (task.parentId == 0 && task.status != null) {
                    Jed.Status status = Jed.Status.getEnum(task.status);
                    switch (status) {
                        case Canceled:
                            canceled = true;
                            break;
                        case Running:
                            running = true;
                            break;
                        case Failed:
                        case DependencyFailed:
                            failed = true;
                            break;
                        case Done:
                        case Completed:
                            done = true;
                            break;
                        default:
                            break;
                    }
                }
            }
            // If more than 1 parent task use the most "important" state
            // Running > Failed/DependencyFailed > Canceled > Done/Completed
            // But if JED appears to have been killed while running change state to Unknown 
            if(running) {
                retVal = isStale() ? Jed.Status.Unknown : Jed.Status.Running;
            }
			else if(failed) {
				retVal = Jed.Status.Failed;
			}
            else if(canceled) {
                retVal = Jed.Status.Canceled;
            }
			else if(done) {
				retVal = Jed.Status.Done;
			}
			else {
				retVal = Jed.Status.Unknown;
			}
		}
		return retVal;
	}
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");
	
	private static long getMillis(String dateString) {
		long retVal = 0;
		try {
			if(dateString != null) {
				 Date date = sdf.parse(dateString);
				 retVal = date.getTime();
			}
		}
		catch(Exception e) {
		}
		return retVal;
	}
	
	@Override
	public long getStartTime() {
		return getMillis(getStartDate());
	}
	
	@Override
	public String getStartDate() {
		String retVal = "";
		long experimentStartMillis = Long.MAX_VALUE;
		if(tasks != null) {
			for(Task task : tasks) {
				String dateString = task.startTime;
				if(dateString != null) {
					long taskStartMillis = getMillis(dateString);
					if(taskStartMillis < experimentStartMillis) {
						experimentStartMillis = taskStartMillis;
						retVal = dateString;
					}
				}
			}
		}
		return retVal;
	}

	@Override
	public void setFileDate(long value) {
		fileDate = value;
	}
	
	@Override
	public long getFileDate() {
		return fileDate;
	}
	
    private long staleTime = 1000 * 60 * 6;                
    private long staleTimeOld = 1000 * 60 * 60 * 24;
    
    @Override
	public boolean isStale() {
        // If the log file has been removed then the driver has stopped.
        // If the lock file is still present the driver may have been killed, so check the age of the state file.
        // If an old version-less file then may have a long job running so wait 24 hrs - new ones are touched every 5 mins
        // Check that the lock-file exists by reading it as the user if necessary,
        boolean logLocked = null != ExperimentsRegistryUtilities.getFileContents(user, directory + "/DRIVER.log.lck");
        if (logLocked) {
            long now = System.currentTimeMillis();
            long fileDate = ExperimentsRegistryUtilities.getFileDate(this);
            if (fileDate > 0) {
                if (fileDate < now) {
                    long elapsed = now - fileDate;
                    long tStale = (version == 0) ? staleTimeOld : staleTime; 
                    if (elapsed < tStale) {
                        return false;
                    }
                }
            }
        }
        return true;
	}
	
	@Override
	public int hashCode() {
		return directory.hashCode();
	}
	
	@Override
	public int compareTo(Object object) {
		int retVal = 0;
		if(object != null) {
			if(object instanceof Experiment) {
				Experiment that = (Experiment)object;
				if(retVal == 0) {
					retVal = compareState(that);
				}
				if(retVal == 0) {
					retVal = compareStartDate(that);
				}
				if(retVal == 0) {
					retVal = compareDirectory(that);
				}
				if(retVal == 0) {
					retVal = compareUser(that);
				}
			}
		}
		return retVal;
	}
	
	private int compareState(Experiment that) {
		int retVal = 0;
		if(this.isActive()) {
			if(that.isActive()) {
				//retVal = 0;
			}
			else {
				retVal = -1;
			}
		}
		else {
			if(that.isActive()) {
				retVal = 1;
			}
			else {
				//retVal = 0;
			}
		}
		return retVal;
	}
	
	private int compareStrings(String s0, String s1) {
		int retVal = 0;
		if(s0 != null) {
			if(s1 != null) {
				retVal = s1.compareTo(s0);
			}
		}
		return retVal;
	}
	
	private int compareStartDate(Experiment that) {
		int retVal = 0;
		if(that != null) {
			retVal = compareStrings(this.getStartDate(), that.getStartDate());
		}
		return retVal;
	}
	
	private int compareDirectory(Experiment that) {
		int retVal = 0;
		if(that != null) {
			retVal = compareStrings(this.getDirectory(), that.getDirectory());
		}
		return retVal;
	}
	
	private int compareUser(Experiment that) {
		int retVal = 0;
		if(that != null) {
			retVal = compareStrings(this.getUser(), that.getUser());
		}
		return retVal;
	}

}
