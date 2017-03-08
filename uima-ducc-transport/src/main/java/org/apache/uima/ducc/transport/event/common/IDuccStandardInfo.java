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

import java.io.Serializable;

public interface IDuccStandardInfo extends Serializable {

	public String getUser();
	public void setUser(String user);

	public String getSubmitter();
	public void setSubmitter(String submitter);

	public String getDateOfSubmission();
	public void setDateOfSubmission(String dateOfSubmission);

	public String getCancelUser();
	public void setCancelUser(String user);

	public String getDateOfCompletion();
	public void setDateOfCompletion(String dateOfCompletion);

	public String getDateOfShutdownProcesses();
	public void setDateOfShutdownProcesses(String dateOfShutdownProcesses);

	public long getDateOfSubmissionMillis();
	public long getDateOfSubmissionMillis(long otherwise);
	public void setDateOfSubmissionMillis(long dateOfSubmission);

	public long getDateOfCompletionMillis();
	public long getDateOfCompletionMillis(long otherwise);
	public void setDateOfCompletionMillis(long dateOfCompletion);

	public long getDateOfShutdownProcessesMillis();
	public long getDateOfShutdownProcessesMillis(long otherwise);
	public void setDateOfShutdownProcessesMillis(long dateOfShutdownProcesses);

	public String getDescription();
	public void setDescription(String description);

	public String getLogDirectory();
	public void setLogDirectory(String logDirectory);

	public String getUmask();
	public void setUmask(String umask);

	public String getWorkingDirectory();
	public void setWorkingDirectory(String workingDirectory);

	public String[] getNotifications();
	public void setNotifications(String[] notifications);

	// JP (or SP) maximum initialization time, in milliseconds
	public long getProcessInitializationTimeMax();
	public void setProcessInitializationTimeMax(long value);
}
