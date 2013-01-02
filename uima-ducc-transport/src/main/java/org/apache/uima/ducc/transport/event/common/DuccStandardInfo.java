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
package org.apache.uima.ducc.transport.event.common;

import java.util.Arrays;

/**
 * Information (mostly) descriptive about the work.
 */
public class DuccStandardInfo implements IDuccStandardInfo {
	
	/**
	 * please increment this sUID when removing or modifying a field 
	 */
	private static final long serialVersionUID = 1L;
	private String user = null;
	private String dateOfSubmission = null;
	private String cancelUser = null;
	private String dateOfCompletion = null;
	private String dateOfShutdownProcesses = null;
	private String description = null;
	private String logDirectory = null;
	private String workingDirectory = null;
	private String[] notifications = null;
	
	@Override
	public String getUser() {
		return user;
	}

	@Override
	public void setUser(String user) {
		this.user = user;
	}

	@Override
	public String getSubmitUser() {
		return getUser();
	}

	@Override
	public void setSubmitUser(String user) {
		setUser(user);
	}

	@Override
	public String getCancelUser() {
		return cancelUser;
	}

	@Override
	public void setCancelUser(String user) {
		this.cancelUser = user;
	}
	
	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String getDateOfSubmission() {
		return dateOfSubmission;
	}

	@Override
	public void setDateOfSubmission(String dateOfSubmission) {
		this.dateOfSubmission = dateOfSubmission;
	}

	@Override
	public String getDateOfCompletion() {
		return dateOfCompletion;
	}

	@Override
	public void setDateOfCompletion(String dateOfCompletion) {
		this.dateOfCompletion = dateOfCompletion;
	}

	@Override
	public String getDateOfShutdownProcesses() {
		return dateOfShutdownProcesses;
	}

	@Override
	public void setDateOfShutdownProcesses(String dateOfShutdownProcesses) {
		this.dateOfShutdownProcesses = dateOfShutdownProcesses;
	}
	
	@Override
	public long getDateOfSubmissionMillis(long otherwise) {
		long millis = otherwise;
		try {
			millis = Long.parseLong(this.dateOfSubmission);
		} catch (Exception e) {
		}
		return millis;
	}

	@Override
	public long getDateOfSubmissionMillis() {
		return getDateOfSubmissionMillis(0);
	}
	
	@Override
	public void setDateOfSubmissionMillis(long dateOfSubmission) {
		this.dateOfSubmission = ""+dateOfSubmission;
	}

	@Override
	public long getDateOfCompletionMillis(long otherwise) {
		long millis = otherwise;
		try {
			millis = Long.parseLong(this.dateOfCompletion);
		} catch (Exception e) {
		}
		return millis;
	}

	@Override
	public long getDateOfCompletionMillis() {
		return getDateOfCompletionMillis(0);
	}
	
	@Override
	public void setDateOfCompletionMillis(long dateOfCompletion) {
		this.dateOfCompletion = ""+dateOfCompletion;
	}

	@Override
	public long getDateOfShutdownProcessesMillis(long otherwise) {
		long millis = otherwise;
		try {
			millis = Long.parseLong(this.dateOfShutdownProcesses);
		} catch (Exception e) {
		}
		return millis;
	}

	@Override
	public long getDateOfShutdownProcessesMillis() {
		return getDateOfShutdownProcessesMillis(0);
	}
	
	@Override
	public void setDateOfShutdownProcessesMillis(long dateOfShutdownProcesses) {
		this.dateOfShutdownProcesses = ""+dateOfShutdownProcesses;
	}
	
	@Override
	public String getLogDirectory() {
		return logDirectory;
	}

	@Override
	public void setLogDirectory(String logDirectory) {
		this.logDirectory = logDirectory;
	}

	@Override
	public String getWorkingDirectory() {
		return workingDirectory;
	}

	@Override
	public void setWorkingDirectory(String workingDirectory) {
		this.workingDirectory = workingDirectory;
	}
	
	@Override
	public String[] getNotifications() {
		return notifications;
	}

	@Override
	public void setNotifications(String[] notifications) {
		this.notifications = notifications;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((dateOfShutdownProcesses == null) ? 0 : dateOfShutdownProcesses.hashCode());
		result = prime
				* result
				+ ((dateOfCompletion == null) ? 0 : dateOfCompletion.hashCode());
		result = prime
				* result
				+ ((dateOfSubmission == null) ? 0 : dateOfSubmission.hashCode());
		result = prime * result
				+ ((description == null) ? 0 : description.hashCode());
		result = prime * result
				+ ((logDirectory == null) ? 0 : logDirectory.hashCode());
		result = prime * result + Arrays.hashCode(notifications);
		result = prime * result + ((user == null) ? 0 : user.hashCode());
		result = prime
				* result
				+ ((workingDirectory == null) ? 0 : workingDirectory.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DuccStandardInfo other = (DuccStandardInfo) obj;
		if (dateOfShutdownProcesses == null) {
			if (other.dateOfShutdownProcesses != null)
				return false;
		} else if (!dateOfShutdownProcesses.equals(other.dateOfShutdownProcesses))
			return false;
		if (dateOfCompletion == null) {
			if (other.dateOfCompletion != null)
				return false;
		} else if (!dateOfCompletion.equals(other.dateOfCompletion))
			return false;
		if (dateOfSubmission == null) {
			if (other.dateOfSubmission != null)
				return false;
		} else if (!dateOfSubmission.equals(other.dateOfSubmission))
			return false;
		if (description == null) {
			if (other.description != null)
				return false;
		} else if (!description.equals(other.description))
			return false;
		if (logDirectory == null) {
			if (other.logDirectory != null)
				return false;
		} else if (!logDirectory.equals(other.logDirectory))
			return false;
		if (!Arrays.equals(notifications, other.notifications))
			return false;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		if (workingDirectory == null) {
			if (other.workingDirectory != null)
				return false;
		} else if (!workingDirectory.equals(other.workingDirectory))
			return false;
		return true;
	}
	
	// **********
	
//	@Override
//	public int hashCode() {
//		final int prime = 31;
//		int result = 1;
//		result = prime * result + ((getUser() == null) ? 0 : getUser().hashCode());
//		result = prime * result + ((getDateOfSubmission() == null) ? 0 : getDateOfSubmission().hashCode());
//		result = prime * result + ((getDateOfCompletion() == null) ? 0 : getDateOfCompletion().hashCode());
//		result = prime * result + ((getDescription() == null) ? 0 : getDescription().hashCode());
//		result = prime * result + ((getLogDirectory () == null) ? 0 : getLogDirectory ().hashCode());
//		result = prime * result + ((getWorkingDirectory () == null) ? 0 : getWorkingDirectory ().hashCode());
//		result = prime * result + ((getNotifications() == null) ? 0 : getNotifications().hashCode());
//		result = prime * result + super.hashCode();
//		return result;
//	}
//	
//	public boolean equals(Object obj) {
//		boolean retVal = false;
//		if(this == obj) {
//			retVal = true;
//		}
//		else if(getClass() == obj.getClass()) {
//			DuccStandardInfo that = (DuccStandardInfo)obj;
//			if(		Util.compare(this.getDateOfCompletion(),that.getDateOfCompletion()) 
//				&&	Util.compare(this.getDescription(),that.getDescription()) 
//				&&	Util.compare(this.getNotifications(),that.getNotifications()) 
//			//	These don't change:
//			//	&&	Util.compare(this.getUser(),that.getUser()) 
//			//	&&	Util.compare(this.getDateOfSubmission(),that.getDateOfSubmission()) 
//			//	&&	Util.compare(this.getOutputDirectory(),that.getOutputDirectory()) 
////				&&	super.equals(obj)
//				) 
//			{
//				retVal = true;
//			}
//		}
//		return retVal;
//	}
	
	
}
