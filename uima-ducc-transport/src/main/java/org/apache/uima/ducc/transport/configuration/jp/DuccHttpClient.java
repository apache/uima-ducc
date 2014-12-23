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
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;
//import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.pool.BasicConnPool;
import org.apache.http.impl.pool.BasicPoolEntry;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;
import org.apache.http.util.EntityUtils;
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
    //PostMethod postMethod;
	String jdUrl;
	
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
    //    postMethod = new PostMethod(jdUrl);
		this.jdUrl = jdUrl;
		pid = getProcessIP("N/A");
		nodeIdentity = new NodeIdentity();
		MultiThreadedHttpConnectionManager cMgr =
		    new MultiThreadedHttpConnectionManager();
		httpClient = 
    		new HttpClient(cMgr);
		 
	}
	public void intialize(String url, int port, String application)
			throws Exception {
		target = application;
		this.timeout = timeout;
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
		connStrategy = new DefaultConnectionReuseStrategy();//DefaultConnectionReuseStrategy.INSTANCE;
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
    private void addCommonHeaders( BasicHttpRequest request ) {
    	request.setHeader("IP", getIP());
		request.setHeader("Hostname", getNodeName());
		request.setHeader("ThreadID",
				String.valueOf(Thread.currentThread().getId()));
		request.setHeader("PID", pid);
		
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

	public IMetaCasTransaction get(IMetaCasTransaction transaction) throws Exception {
		// According to HTTP spec, GET request should not include the body. We need
		// to send in body to the JD so use POST
		BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest("POST", target);
//		BasicHttpRequest request = new BasicHttpRequest("GET", target);
		addCommonHeaders(transaction);
		addCommonHeaders(request);
		return execute(request, transaction);
	}
	public IMetaCasTransaction post(IMetaCasTransaction transaction) throws Exception {
		BasicHttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest("POST", target);
		addCommonHeaders(transaction);
		addCommonHeaders(request);
		transaction.setDirection(Direction.Request);
		return execute(request,transaction);
	}
	private IMetaCasTransaction execute( BasicHttpEntityEnclosingRequest request, IMetaCasTransaction transaction ) throws Exception {
		BasicPoolEntry poolEntry=null;
		int retry = 2;
		Exception lastError = null;
		IMetaCasTransaction reply=null;

		while( retry-- > 0 ) {
			try {
				// Get the connection from the pool
			    Future<BasicPoolEntry> future = connPool.lease(host,  null);
			    poolEntry = future.get();
			    
			    HttpClientConnection conn = poolEntry.getConnection();
			    coreContext.setAttribute(HttpCoreContext.HTTP_CONNECTION, conn);
	            coreContext.setAttribute(HttpCoreContext.HTTP_TARGET_HOST, host);
	            
	            conn.setSocketTimeout(10000); 
				System.out.println(">> Request URI: "	+ request.getRequestLine().getUri());
	           // request.

				request.setHeader("content-type", "text/xml");
	            String body = XStreamUtils.marshall(transaction);
				HttpEntity entity = new StringEntity(body);

				request.setEntity(entity);

				httpexecutor.preProcess(request, httpproc, coreContext);
				HttpResponse response = httpexecutor.execute(request, conn,	coreContext);
				httpexecutor.postProcess(response, httpproc, coreContext);

				if ( response.getStatusLine().getStatusCode() != 200) {
					System.out.println("Unable to Communicate with JD - Error:"+response.getStatusLine());
					throw new RuntimeException("JP Http Client Unable to Communicate with JD - Error:"+response.getStatusLine());
				}
				System.out.println("<< Response: "
						+ response.getStatusLine());
				String responseData = EntityUtils.toString(response.getEntity());
				System.out.println(responseData);
				Object o = XStreamUtils.unmarshall(responseData);
				if ( o instanceof IMetaCasTransaction) {
					reply = (MetaCasTransaction)o;
					break;
				} else {
					throw new InvalidClassException("Expected IMetaCasTransaction - Instead Received "+o.getClass().getName());
				}
			} catch( Exception t) {
				lastError = t;
				t.printStackTrace();
			}
			finally {
				System.out.println("==============");
				connPool.release(poolEntry, true);
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
	            // wait for a reply
	            httpClient.executeMethod(postMethod);
                String responseData = postMethod.getResponseBodyAsString();	            
				if ( postMethod.getStatusLine().getStatusCode() != 200) {
					logger.error("execute", null, "Unable to Communicate with JD - Error:"+postMethod.getStatusLine());
					throw new RuntimeException("JP Http Client Unable to Communicate with JD - Error:"+postMethod.getStatusLine());
				}
				logger.info("execute", null, "Thread:"+Thread.currentThread().getId()+" JD Reply Status:"+postMethod.getStatusLine());
				logger.info("execute", null, "Thread:"+Thread.currentThread().getId()+" Recv'd:"+responseData);
				Object o = XStreamUtils.unmarshall(responseData);
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