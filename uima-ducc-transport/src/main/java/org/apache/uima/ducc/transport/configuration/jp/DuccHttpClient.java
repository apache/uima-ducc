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
package org.apache.uima.ducc.transport.configuration.jp;
import java.io.InvalidClassException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpHost;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.pool.BasicConnPool;
import org.apache.http.impl.pool.BasicPoolEntry;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Direction;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;

public class DuccHttpClient {
	DuccLogger logger = new DuccLogger(DuccHttpClient.class);
	HttpRequestExecutor httpexecutor = null;
	ConnectionReuseStrategy connStrategy = null;
	HttpCoreContext coreContext = null;
	HttpProcessor httpproc = null;
	BasicConnPool connPool = null;
	HttpHost host = null;
	String target = null;
	NodeIdentity nodeIdentity;
	String pid = "";
	ReentrantLock lock = new ReentrantLock();
	int timeout;
	
	// New --------------------
    HttpClient httpClient = null;
	String jdUrl;
	MultiThreadedHttpConnectionManager cMgr = null;
	
	public void setTimeout( int timeout) {
		this.timeout = timeout;
	}
	public void setScaleout(int scaleout) {
		connPool.setMaxTotal(scaleout);
		connPool.setDefaultMaxPerRoute(scaleout);
		connPool.setMaxPerRoute(host, scaleout);
	}
	public String getJdUrl() {
		return jdUrl;
	}
	public void initialize(String jdUrl) throws Exception {
		this.jdUrl = jdUrl;
		pid = getProcessIP("N/A");
		nodeIdentity = new NodeIdentity();
		cMgr = new MultiThreadedHttpConnectionManager();
		
		httpClient = 
    		new HttpClient(cMgr);
	 
	}
	public void stop() throws Exception {
		if ( cMgr != null ) {
			cMgr.shutdown();
		}
	}
	public void intialize(String url, int port, String application)
			throws Exception {
		target = application;
		httpproc = HttpProcessorBuilder.create().add(new RequestContent())
				.add(new RequestTargetHost()).add(new RequestConnControl())
				.add(new RequestUserAgent("Test/1.1"))
				.add(new org.apache.http.protocol.RequestExpectContinue(true))
				.build();
		
		httpexecutor = new HttpRequestExecutor();

		coreContext = HttpCoreContext.create();
		host = new HttpHost(url, port);
		coreContext.setTargetHost(host);
		connPool = new BasicConnPool();
		connStrategy = new DefaultConnectionReuseStrategy();
		pid = getProcessIP("N/A");
		nodeIdentity = new NodeIdentity();
		
		// test connection to the JD
		testConnection();
		System.out.println("HttpClient Initialized");
	}
	public void testConnection() throws Exception {
		// test connection to the JD
	    Future<BasicPoolEntry> future = connPool.lease(host,  null);
		BasicPoolEntry poolEntry = null;
		try {
			poolEntry= future.get();
		} finally {
			connPool.release(poolEntry, true);
		}
	}
	public void close() {
    	try {
        //	conn.close();
    		
    	} catch( Exception e) {
    		e.printStackTrace();
    	}
    }
	private String getProcessIP(final String fallback) {
		// the following code returns '<pid>@<hostname>'
		String name = ManagementFactory.getRuntimeMXBean().getName();
		int pos = name.indexOf('@');

		if (pos < 1) {
			// pid not found
			return fallback;
		}

		try {
			return Long.toString(Long.parseLong(name.substring(0, pos)));
		} catch (NumberFormatException e) {
			// ignore
		}
		return fallback;
	}
	private String getIP() {
		String ip =nodeIdentity.getIp();
		if ( System.getenv("IP") != null) {
			ip = System.getenv("IP");
		}
		return ip;
	}
	private String getNodeName() {
		String nn =nodeIdentity.getName();
		if ( System.getenv("NodeName") != null) {
			nn = System.getenv("NodeName");
		}
		return nn;
	}
	private String getProcessName() {
		String pn = System.getenv("ProcessDuccIdFriendly");
		return pn;
	}
    private void addCommonHeaders( IMetaCasTransaction transaction ) {
    	String location = "addCommonHeaders";
    	transaction.setRequesterAddress(getIP());
    	transaction.setRequesterNodeName(getNodeName());
    	transaction.setRequesterProcessName(getProcessName());
    	transaction.setRequesterProcessId(Integer.valueOf(pid));
    	transaction.setRequesterThreadId((int)Thread.currentThread().getId());
    	logger.trace(location, null, "ip:"+transaction.getRequesterAddress());
    	logger.trace(location, null, "nodeName:"+transaction.getRequesterNodeName());
    	logger.trace(location, null, "processName:"+transaction.getRequesterProcessName());
    	logger.trace(location, null, "processId:"+transaction.getRequesterProcessId());
    	logger.trace(location, null, "threadId:"+transaction.getRequesterThreadId());
    }
    
    private void addCommonHeaders( PostMethod method ) {
    	synchronized( DuccHttpClient.class) {
        	method.setRequestHeader("IP", getIP());
        	method.setRequestHeader("Hostname", getNodeName());
        	method.setRequestHeader("ThreadID",
    				String.valueOf(Thread.currentThread().getId()));
        	method.setRequestHeader("PID", pid);
    	}
		
    }

	public IMetaCasTransaction execute( IMetaCasTransaction transaction, PostMethod postMethod ) throws Exception {
		int retry = 2;
		Exception lastError = null;
		IMetaCasTransaction reply=null;

		addCommonHeaders(transaction);
		transaction.setDirection(Direction.Request);
		
		while( retry-- > 0 ) {
			try {
				// Serialize request object to XML
				String body = XStreamUtils.marshall(transaction);
	            RequestEntity e = new StringRequestEntity(body,"application/xml","UTF-8" );
	            
	            postMethod.setRequestEntity(e);
	            
	            addCommonHeaders(postMethod);
	    
	            postMethod.setRequestHeader("Content-Length", String.valueOf(body.length()));
	            logger.debug("execute",null, "calling httpClient.executeMethod()");
	            // wait for a reply
	            httpClient.executeMethod(postMethod);
	            logger.debug("execute",null, "httpClient.executeMethod() returned");
                
                String content = new String(postMethod.getResponseBody());
                
				if ( postMethod.getStatusLine().getStatusCode() != 200) {
					logger.error("execute", null, "Unable to Communicate with JD - Error:"+postMethod.getStatusLine());
					logger.error("execute", null, "Content causing error:"+postMethod.getResponseBody());
					System.out.println("Thread::"+Thread.currentThread().getId()+" ERRR::Content causing error:"+postMethod.getResponseBody());
					throw new RuntimeException("JP Http Client Unable to Communicate with JD - Error:"+postMethod.getStatusLine());
				}
				logger.debug("execute", null, "Thread:"+Thread.currentThread().getId()+" JD Reply Status:"+postMethod.getStatusLine());
				logger.debug("execute", null, "Thread:"+Thread.currentThread().getId()+" Recv'd:"+content);
				Object o;
				try {
					o = XStreamUtils.unmarshall(content); //sb.toString());
					
				} catch( Exception ex) {
					logger.error("execute", null, "Thread:"+Thread.currentThread().getId()+" ERRR::Content causing error:"+postMethod.getResponseBody(),ex);
					throw ex;
				}
				if ( o instanceof IMetaCasTransaction) {
					reply = (MetaCasTransaction)o;
					break;
				} else {
					throw new InvalidClassException("Expected IMetaCasTransaction - Instead Received "+o.getClass().getName());
				}
			} catch( Exception t) {
				lastError = t;
				logger.error("run", null, t);
			}
			finally {
				postMethod.releaseConnection();
			}
			
		}
		if ( reply != null ) {
			return reply;
		} else {
			if ( lastError != null ){
				throw lastError;

			} else {
				throw new RuntimeException("Shouldn't happen ");
			}
		} 
	}
	
}