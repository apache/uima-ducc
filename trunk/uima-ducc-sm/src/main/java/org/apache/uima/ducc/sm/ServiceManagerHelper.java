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
package org.apache.uima.ducc.sm;

import java.io.File;
import java.io.FileInputStream;
import java.util.NavigableSet;
import java.util.Properties;

import org.apache.uima.ducc.common.persistence.services.IStateServices;
import org.apache.uima.ducc.common.persistence.services.StateServicesDirectory;
import org.apache.uima.ducc.common.persistence.services.StateServicesFactory;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;
import org.apache.uima.ducc.common.utils.id.IDuccIdFactory;

public class ServiceManagerHelper {

	private static DuccLogger logger = DuccLogger.getLogger(ServiceManagerHelper.class.getName(), SmConstants.COMPONENT_NAME);
	private static DuccId jobid = null;

	private static String service_seqno = IStateServices.sequenceKey;
	
	/**
	 * construct DuccIdFactory for services registry
	 */
	public static IDuccIdFactory getDuccIdFactory() {
		String location = "getDuccIdFactory";
		long fileSeqNo = getFileSeqNo()+1;
		long dbSeqNo = getDbSeqNo()+1;
		long seqNo = 0;
		if(fileSeqNo > seqNo) {
			seqNo = fileSeqNo;
		}
		if(dbSeqNo > seqNo) {
			seqNo = dbSeqNo;
		}
		logger.info(location, jobid, "seqNo:", seqNo,  "dbSeqNo:", dbSeqNo, "fileSeqNo:", fileSeqNo);
		IDuccIdFactory duccIdFactory = new DuccIdFactory(seqNo);
		return duccIdFactory;
	}

	private static long getFileSeqNo() {
		String location = "getFileSeqNo";
		long seqNo = -1;
		try {
			String state_dir = System.getProperty("DUCC_HOME") + "/state";
			String state_file = state_dir + "/sm.properties";

			Properties sm_props = new DuccProperties();
			File sf = new File(state_file);
			FileInputStream fos;
			if (sf.exists()) {
				fos = new FileInputStream(state_file);
				try {
					sm_props.load(fos);
					String s = sm_props.getProperty(service_seqno);
					seqNo = Integer.parseInt(s) + 1;
				} finally {
					fos.close();
				}
			}
		}
		catch(Exception e) {
			logger.error(location, jobid, e);
		}
		return seqNo;
	}

	/**
	 * fetch largest service sequence number from database
	 */
	public static long getDbSeqNo() {
		String location = "getDbSeqNo";
		long seqno = -1;
		try {
			IStateServices iss = StateServicesFactory.getInstance(
					ServiceManagerHelper.class.getName(),
					SmConstants.COMPONENT_NAME);
			StateServicesDirectory ssd = iss.getStateServicesDirectory();
			NavigableSet<Long> keys = ssd.getDescendingKeySet();
			if (!keys.isEmpty()) {
				seqno = keys.first();
			}
		} catch (Exception e) {
			logger.error(location, jobid, e);
		}
		return seqno;
	}
}
