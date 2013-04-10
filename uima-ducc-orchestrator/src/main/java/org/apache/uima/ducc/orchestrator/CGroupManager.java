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

import org.apache.uima.ducc.common.utils.id.IDuccId;
import org.apache.uima.ducc.transport.event.common.CGroup;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;

public class CGroupManager {

	public static void assign(IDuccId primaryId, IDuccProcess process, long max_size_in_bytes) {
		IDuccId secondaryId = process.getDuccId();
		CGroup cgroup = new CGroup(primaryId, secondaryId, max_size_in_bytes);
		process.setCGroup(cgroup);
	}
}
