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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.ducc.common.IDuccEnv;

public class DuccPropertiesResolver {
	
	//private DuccId duccId = null;
	
	private AtomicBoolean loadedPrivate = new AtomicBoolean(false);
	
	private AtomicBoolean e1 = new AtomicBoolean(false);
	private AtomicBoolean e2 = new AtomicBoolean(false);
	private AtomicBoolean e3 = new AtomicBoolean(false);
	private AtomicBoolean e4 = new AtomicBoolean(false);
	private AtomicBoolean e5 = new AtomicBoolean(false);
	
    private static DuccPropertiesResolver duccPropertiesResolver = new DuccPropertiesResolver();
    
    public static DuccPropertiesResolver getInstance() {
        return duccPropertiesResolver;
    }

    public static String get(String key) {
        return duccPropertiesResolver.getProperty(key);
    }
    
    public static String get(String key, String dflt) {
        String value = duccPropertiesResolver.getProperty(key);
        return value != null ? value : dflt;
    }
    
    public static int get(String key, int dflt) {
    	int retVal = dflt;
        String value = duccPropertiesResolver.getProperty(key);
        try {
        	retVal = Integer.parseInt(value);
        }
        catch(Exception e) {
        }
        return retVal;
    }
    
    private Properties initialProperties = new DuccProperties();
    
    private Properties defaultProperties = new DuccProperties();
    
    public DuccPropertiesResolver() {
    	try {
    		init(initialProperties);
            initDefaultProperties();
    	}
    	catch(Throwable t) {
    		t.printStackTrace();
    	}
        
    }
    
    public static final String ducc_head = "ducc.head";
    
    public static final String ducc_security_home = "ducc.security.home";
    
    public static final String ducc_private_resources = "ducc.private.resources";
    
    //Legacy, replaced by: ducc_job_max_pipelines_count
    public static final String ducc_threads_limit = "ducc.threads.limit";
    
    //<UIMA-4612>
    public static final String ducc_job_max_pipelines_count = "ducc.job.max.pipelines.count";
    //</UIMA-4612>
    
    public static final String ducc_driver_jvm_args = "ducc.driver.jvm.args";
    public static final String ducc_process_jvm_args = "ducc.process.jvm.args";
    public static final String ducc_environment_propagated = "ducc.environment.propagated";
    
    public static final String ducc_runmode = "ducc.runmode";
    public static final String ducc_signature_required = "ducc.signature.required";
    public static final String ducc_broker_url = "ducc.broker.url";
    public static final String ducc_broker_protocol = "ducc.broker.protocol";
    public static final String ducc_broker_port = "ducc.broker.port";
    public static final String ducc_broker_url_decoration = "ducc.broker.url.decoration";
    public static final String ducc_broker_name = "ducc.broker.name";
    public static final String ducc_broker_jmx_port = "ducc.broker.jmx.port";
    public static final String ducc_jms_provider = "ducc.jms.provider";
    
    public static final String ducc_database_automanage = "ducc.database.automanage";
    public static final String ducc_database_host_list = "ducc.database.host.list";
    public static final String ducc_database_disabled = "--disabled--";
    public static final String ducc_database_jmx_host = "ducc.database.jmx.host";
    public static final String ducc_database_jmx_port = "ducc.database.jmx.port";
    
    public static final String ducc_orchestrator_state_publish_rate = "ducc.orchestrator.state.publish.rate";
    public static final String ducc_orchestrator_state_update_endpoint_type = "ducc.orchestrator.state.update.endpoint.type";
    public static final String ducc_orchestrator_state_update_endpoint = "ducc.orchestrator.state.update.endpoint";
    public static final String ducc_orchestrator_http_port = "ducc.orchestrator.http.port";
    public static final String ducc_orchestrator_http_node = "ducc.orchestrator.http.node";
    public static final String ducc_orchestrator_maintenance_rate = "ducc.orchestrator.maintenance.rate";
    public static final String ducc_orchestrator_job_factory_classpath_order = "ducc.orchestrator.job.factory.classpath.order";    
    public static final String ducc_orchestrator_unmanaged_reservations_accepted = "ducc.orchestrator.unmanaged.reservations.accepted";  
    public static final String ducc_orchestrator_use_lock_file = "ducc.orchestrator.use.lock.file";  
    public static final String ducc_orchestrator_start_type = "ducc.orchestrator.start.type";  
    public static final String ducc_orchestrator_checkpoint = "ducc.orchestrator.checkpoint";  
    
    public static final String ducc_default_process_per_item_time_max = "ducc.default.process.per.item.time.max";
    public static final String ducc_default_process_init_time_max = "ducc.default.process.init.time.max";
    
    public static final String ducc_jd_queue_prefix = "ducc.jd.queue.prefix";
    
    public static final String ducc_jd_host_class = "ducc.jd.host.class";
    public static final String ducc_jd_host_description = "ducc.jd.host.description";
    public static final String ducc_jd_host_memory_size = "ducc.jd.host.memory.size";
    public static final String ducc_jd_host_number_of_machines = "ducc.jd.host.number.of.machines";
    public static final String ducc_jd_host_user = "ducc.jd.host.user";
    
    public static final String ducc_jd_state_publish_rate = ducc_orchestrator_state_publish_rate;
    
    public static final String ducc_jd_configuration_class = "ducc.jd.configuration.class";
    public static final String ducc_jd_startup_initialization_error_limit = "ducc.jd.startup.initialization.error.limit";
    
    public static final String ducc_jd_workitem_name_maximum_length = "ducc.jd.workitem.name.maximum.length";
    
    public static final String ducc_jd_error_handler_class = "ducc.jd.error.handler.class";
    public static final String ducc_jd_error_handler_args = "ducc.jd.error.handler.args";
    
    public static final String ducc_rm_class_definitions = "ducc.rm.class.definitions";
    public static final String ducc_rm_user_registry = "ducc.rm.user.registry"; // UIMA-4275
    public static final String ducc_rm_share_quantum = "ducc.rm.share.quantum";
    public static final String ducc_jd_share_quantum = "ducc.jd.share.quantum";
    public static final String ducc_jd_share_quantum_reserve_count = "ducc.jd.share.quantum.reserve.count";
  
    public static final String ducc_authentication_implementer = "ducc.authentication.implementer";
    public static final String ducc_authentication_users_include = "ducc.authentication.users.include";
    public static final String ducc_authentication_users_exclude = "ducc.authentication.users.exclude";
    public static final String ducc_authentication_groups_include = "ducc.authentication.groups.include";
    public static final String ducc_authentication_groups_exclude = "ducc.authentication.groups.exclude";
    
    public static final String ducc_ws_host = "ducc.ws.node";
    public static final String ducc_ws_port = "ducc.ws.port";
    public static final String ducc_ws_max_history_entries = "ducc.ws.max.history.entries";
    public static final String ducc_ws_login_enabled = "ducc.ws.login.enabled";
    public static final String ducc_ws_bytes_per_page = "ducc.ws.bytes.per.page";
    public static final String ducc_ws_banner_message = "ducc.ws.banner.message";
    public static final String ducc_ws_user_data_access = "ducc.ws.user.data.access";
    
    public static final String ducc_rm_node_stability = "ducc.rm.node.stability";
    public static final String ducc_agent_node_metrics_publish_rate = "ducc.agent.node.metrics.publish.rate";
  
    public static final String ducc_agent_node_inventory_publish_rate ="ducc.agent.node.inventory.publish.rate";
    public static final String ducc_agent_node_inventory_publish_rate_skip ="ducc.agent.node.inventory.publish.rate.skip";
    
    public static final String ducc_transport_trace = "ducc.transport.trace";
    
    public static final String ducc_ws_port_ssl = "ducc.ws.port.ssl";
    public static final String ducc_ws_welcome_page = "ducc.ws.welcome.page";
    public static final String ducc_ws_idle_timeout = "ducc.ws.idle.timeout";
    public static final String ducc_ws_max_threads = "ducc.ws.max.threads";
    public static final String ducc_ws_monitored_daemon_down_millis_expiry = "ducc.ws.monitored.daemon.down.millis.expiry";

    private void initDefaultProperties() {
        defaultProperties.put(ducc_runmode,"Production");
        defaultProperties.put(ducc_broker_url,"tcp://localhost:61616");
        defaultProperties.put(ducc_jms_provider,"activemq");
        defaultProperties.put(ducc_orchestrator_state_update_endpoint,"ducc.orchestrator.state");    
        defaultProperties.put(ducc_orchestrator_state_update_endpoint_type,"topic");
        defaultProperties.put(ducc_default_process_init_time_max,"1440");
        defaultProperties.put(ducc_default_process_per_item_time_max,"1");
        defaultProperties.put(ducc_rm_share_quantum,"10");
        defaultProperties.put(ducc_jd_share_quantum,"300");
        defaultProperties.put(ducc_jd_share_quantum_reserve_count,"2");
        defaultProperties.put(ducc_orchestrator_unmanaged_reservations_accepted,"true");
        defaultProperties.put(ducc_orchestrator_use_lock_file,"false");
        defaultProperties.put(ducc_ws_login_enabled,"true");
        defaultProperties.put(ducc_authentication_implementer,"org.apache.uima.ducc.ws.authentication.LinuxAuthenticationManager");
        defaultProperties.put(ducc_jd_state_publish_rate,""+1000*15*1);
        defaultProperties.put(ducc_jd_host_class,"JobDriver");
        defaultProperties.put(ducc_jd_host_description,"Job Driver");
        defaultProperties.put(ducc_jd_host_memory_size,"2GB");
        defaultProperties.put(ducc_jd_host_number_of_machines,"1");
        defaultProperties.put(ducc_jd_host_user,"System");
    }
    
    private void init(Properties properties) {
    	//String location = "init";
    	String fileName = null;
        try {
        	String componentProperties="ducc.deploy.configuration";
        	fileName = System.getProperty(componentProperties);
        	if(fileName == null) {
        		fileName = IDuccEnv.DUCC_PROPERTIES_FILE;
        	}
            File file = new File(fileName);
            FileInputStream fis;
            fis = new FileInputStream(file);
            properties.load(fis);
            fis.close();
        } 
        catch (FileNotFoundException e) {
        	if(!e1.get()) {
            	//logger.error(location, duccId, "File not found: "+fileName);
            	e1.set(true);;
            }
        } 
        catch (IOException e) {
        	if(!e2.get()) {
            	//logger.debug(location, duccId, "Error reading file: "+fileName);
            	e2.set(true);;
            }
        }
        enrich(properties);
    }
    
    private void enrich(Properties properties) {
    	// add or override with ducc.private.properties
        Properties privateProperties = getPrivateProperties();
        for(Entry<Object, Object> entry : privateProperties.entrySet()) {
        	properties.put(entry.getKey(), entry.getValue());
        }
    }
    
    private Properties getPrivateProperties() {
    	//String location = "getPrivateProperties";
    	Properties privateProperties = new Properties();
    	String key = ducc_private_resources;
    	String directory = getProperty(key);
    	if(directory != null) {
    		String fileName = directory+File.separator+"ducc.private.properties";
        	try {
                File file = new File(fileName);
                FileInputStream fis;
                fis = new FileInputStream(file);
                privateProperties.load(fis);
                fis.close();
                if(loadedPrivate.get()) {
                	//logger.debug(location, duccId, "Reloaded: "+fileName);
                }
                else {
                	//logger.debug(location, duccId, "Loaded: "+fileName);
                }
                loadedPrivate.set(true);
            } 
            catch (FileNotFoundException e) {
            	if(!e3.get()) {
                	//logger.debug(location, duccId, "File not found: "+fileName);
                	e3.set(true);;
                }
            } 
        	catch (IOException e) {
        		if(!e4.get()) {
                	//logger.debug(location, duccId, "Error reading file: "+fileName);
                	e4.set(true);;
                }
            }
    	}
    	else {
    		if(!e5.get()) {
            	//logger.debug(location, duccId, "Key not found: "+key);
            	e5.set(true);;
            }
    	}
    	return privateProperties;
    }
    
    public String getProperty(String key) {
        return getCachedProperty(key);
    }
    
    public String getCachedProperty(String key) {
        String value = initialProperties.getProperty(key);
        if(value == null) {
            value = defaultProperties.getProperty(key);
        }
        return value==null ? null : value.trim();
    }
    
    public String getFileProperty(String key) {
        Properties currentProperties = new DuccProperties();
        init(currentProperties);
        String value = currentProperties.getProperty(key);
        if(value == null) {
            value = defaultProperties.getProperty(key);
        }
        return value==null ? null : value.trim();
    }
}
