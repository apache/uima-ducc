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
package org.apache.uima.ducc.transport.event.cli;

import java.io.Serializable;

/**
 * Reply characteristics provided by JM.
 */

@SuppressWarnings("serial")
public class JobReplyProperties extends JobRequestProperties implements Serializable {
	
	public static String key_message = "message";
	
	public static String msg_user_not_authorized = "user not authorized";
	public static String msg_job_not_found = "job not found";
	public static String msg_service_not_found = "service not found";
	public static String msg_managed_reservation_not_found = "managed reservation not found";
	public static String msg_process_not_found = "process not found";
	public static String msg_process_not_active = "process not active";
	public static String msg_process_canceled = "process canceled";
	public static String msg_canceled = "canceled";
}
