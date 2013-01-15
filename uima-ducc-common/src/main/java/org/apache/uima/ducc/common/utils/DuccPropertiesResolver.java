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
package org.apache.uima.ducc.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.uima.ducc.common.IDuccEnv;


public class DuccPropertiesResolver {

	private static DuccPropertiesResolver duccPropertiesResolver = new DuccPropertiesResolver();
	
	public static DuccPropertiesResolver getInstance() {
		return duccPropertiesResolver;
	}
	
	private Properties initialProperties = new Properties();
	
	private Properties defaultProperties = new Properties();
	
	public DuccPropertiesResolver() {
		init(initialProperties);
		initDefaultProperties();
	}
	
	public static final String ducc_submit_threads_limit = "ducc.submit.threads.limit";
	public static final String ducc_submit_driver_jvm_args = "ducc.submit.driver.jvm.args";
	public static final String ducc_submit_process_jvm_args = "ducc.submit.process.jvm.args";
	
	public static final String ducc_runmode = "ducc.runmode";
	public static final String ducc_signature_required = "ducc.signature.required";
	public static final String ducc_broker_url = "ducc.broker.url";
	public static final String ducc_broker_protocol = "ducc.broker.protocol";
	public static final String ducc_broker_hostname = "ducc.broker.hostname";
	public static final String ducc_broker_port = "ducc.broker.port";
	public static final String ducc_broker_url_decoration = "ducc.broker.url.decoration";
	public static final String ducc_broker_name = "ducc.broker.name";
	public static final String ducc_broker_jmx_port = "ducc.broker.jmx.port";
	public static final String ducc_jms_provider = "ducc.jms.provider";
	public static final String ducc_orchestrator_request_endpoint_type = "ducc.orchestrator.request.endpoint.type";
	public static final String ducc_orchestrator_request_endpoint = "ducc.orchestrator.request.endpoint";
	public static final String ducc_orchestrator_state_update_endpoint_type = "ducc.orchestrator.state.update.endpoint.type";
	public static final String ducc_orchestrator_state_update_endpoint = "ducc.orchestrator.state.update.endpoint";
	public static final String ducc_orchestrator_http_port = "ducc.orchestrator.http.port";
	public static final String ducc_orchestrator_node = "ducc.orchestrator.node";
	public static final String ducc_orchestrator_maintenance_rate = "ducc.orchestrator.maintenance.rate";
	public static final String default_process_get_meta_time_max = "default.process.get.meta.time.max";
	public static final String default_process_per_item_time_max = "default.process.per.item.time.max";
	
	public static final String ducc_jd_queue_prefix = "ducc.jd.queue.prefix";
	
	public static final String ducc_rm_share_quantum = "ducc.rm.share.quantum";
	
	public static final String ducc_ws_host = "ducc.ws.node";
	public static final String ducc_ws_port = "ducc.ws.port";
	public static final String ducc_ws_authentication_pw = "ducc.ws.authentication.pw";
	public static final String ducc_ws_max_history_entries = "ducc.ws.max.history.entries";
	
	public static final String ducc_agent_node_inventory_publish_rate ="ducc.agent.node.inventory.publish.rate";
	public static final String ducc_agent_node_inventory_publish_rate_skip ="ducc.agent.node.inventory.publish.rate.skip";
	
	public static final String ducc_transport_trace = "ducc.transport.trace";
	
	private void initDefaultProperties() {
		defaultProperties.put(ducc_runmode,"Production");
		defaultProperties.put(ducc_broker_url,"tcp://localhost:61616");
		defaultProperties.put(ducc_jms_provider,"activemq");
		defaultProperties.put(ducc_orchestrator_request_endpoint_type,"queue");
		defaultProperties.put(ducc_orchestrator_state_update_endpoint,"ducc.orchestrator.state");	
		defaultProperties.put(ducc_orchestrator_state_update_endpoint_type,"topic");
		defaultProperties.put(ducc_orchestrator_request_endpoint,"ducc.orchestrator.request");
		defaultProperties.put(default_process_get_meta_time_max,"1");
		defaultProperties.put(default_process_per_item_time_max,"1");
	}
	
	private void init(Properties properties) {
		try {
			File file = new File(IDuccEnv.DUCC_PROPERTIES_FILE);
			FileInputStream fis;
			fis = new FileInputStream(file);
			properties.load(fis);
			fis.close();
		} 
		catch (FileNotFoundException e) {
			System.out.println("File not found: "+IDuccEnv.DUCC_PROPERTIES_FILE);
		} 
		catch (IOException e) {
			System.out.println("Error reading file: "+IDuccEnv.DUCC_PROPERTIES_FILE);
		}
	}
	
	public String getProperty(String key) {
		return getCachedProperty(key);
	}
	
	public String getCachedProperty(String key) {
		String value = initialProperties.getProperty(key);
		if(value == null) {
			value = defaultProperties.getProperty(key);
		}
		return value;
	}
	
	public String getFileProperty(String key) {
		Properties currentProperties = new Properties();
		init(currentProperties);
		String value = currentProperties.getProperty(key);
		if(value == null) {
			value = defaultProperties.getProperty(key);
		}
		return value;
	}
}
