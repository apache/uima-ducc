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
package org.apache.uima.ducc.common.head;

public interface IDuccHead {
	
	public enum DuccHeadState {
		initial,
		master,
		backup,
		unspecified,
	}
	
	public static String initial = DuccHeadState.initial.name();
	public static String master = DuccHeadState.master.name();
	public static String backup = DuccHeadState.backup.name();
	public static String unspecified = DuccHeadState.unspecified.name();
	
	public enum DuccHeadTransition { 
		master_to_master,
		master_to_backup,
		backup_to_backup,
		backup_to_master,
		unspecified,
	};
	
	public String get_ducc_head_mode();
	public boolean is_ducc_head_unspecified();
	public boolean is_ducc_head_reliable();
	public boolean is_ducc_head_master();
	public boolean is_ducc_head_virtual_master();
	public boolean is_ducc_head_backup();
	public DuccHeadTransition transition();
}
