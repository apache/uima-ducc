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
import java.net.InetAddress;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

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
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Direction;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;

public class DuccHttpClient {
	
	HttpRequestExecutor httpexecutor = null;
	ConnectionReuseStrategy connStrategy = null;
	HttpCoreContext coreContext = null;
	HttpProcessor httpproc = null;
	BasicConnPool connPool = null;
	HttpHost host = null;
	String target = null;
//	String hostIP = "";
//	String hostname = "";
	NodeIdentity nodeIdentity;
	String pid = "";
	ReentrantLock lock = new ReentrantLock();
	int timeout;
	
	public void setTimeout( int timeout) {
		this.timeout = timeout;
	}
	public void setScaleout(int scaleout) {
		connPool.setMaxTotal(scaleout);
		connPool.setDefaultMaxPerRoute(scaleout);
		connPool.setMaxPerRoute(host, scaleout);
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
    private void addCommonHeaders( BasicHttpRequest request ) {
    	request.setHeader("IP", nodeIdentity.getIp());
		request.setHeader("Hostname", nodeIdentity.getName());
		request.setHeader("ThreadID",
				String.valueOf(Thread.currentThread().getId()));
		request.setHeader("PID", pid);
		
    }
    private void addCommonHeaders( IMetaCasTransaction transaction ) {
    	transaction.setRequesterAddress(nodeIdentity.getIp());
    	transaction.setRequesterName(nodeIdentity.getName());
    	transaction.setRequesterProcessId(Integer.valueOf(pid));
    	transaction.setRequesterThreadId((int)Thread.currentThread().getId());
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
				return (MetaCasTransaction)o;
			} else {
				throw new InvalidClassException("Expected IMetaCasTransaction - Instead Received "+o.getClass().getName());
			}
		} finally {
			System.out.println("==============");
			connPool.release(poolEntry, true);
		}

	}

}