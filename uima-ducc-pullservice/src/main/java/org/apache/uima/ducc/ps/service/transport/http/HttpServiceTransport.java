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
package org.apache.uima.ducc.ps.service.transport.http;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler.Action;
import org.apache.uima.ducc.ps.service.errors.ServiceException;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.registry.IRegistryClient;
import org.apache.uima.ducc.ps.service.transport.IServiceTransport;
import org.apache.uima.ducc.ps.service.transport.ITargetURI;
import org.apache.uima.ducc.ps.service.transport.TransportException;
import org.apache.uima.ducc.ps.service.transport.TransportStats;
import org.apache.uima.ducc.ps.service.transport.target.NoOpTargetURI;
import org.apache.uima.ducc.ps.service.transport.target.TargetURIFactory;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class HttpServiceTransport implements IServiceTransport {
	private Logger logger =  UIMAFramework.getLogger(HttpServiceTransport.class);
	private HttpClient httpClient = null;
	private PoolingHttpClientConnectionManager cMgr = null;
	private int clientMaxConnections = 1;
	private int clientMaxConnectionsPerRoute = 60;
	private int clientMaxConnectionsPerHostPort = 0;
	private ReentrantLock lock = new ReentrantLock();
//	private CountDownLatch initLatch = new CountDownLatch(1);
	private ReentrantLock registryLookupLock = new ReentrantLock();
    private long threadSleepTime=10000; // millis
    private final String nodeIP;
    private final String nodeName;
    private final String pid;		
    private ITargetURI currentTargetUrl = new NoOpTargetURI();
    private static final  String NA="N/A";
    private TransportStats stats = new TransportStats();
    private IRegistryClient registryClient;
    // holds reference to HttpPost object for every thread. Key=thread id
    private Map<Long,HttpPost> httpPostMap = 
    		new HashMap<>();
    private volatile boolean stopping = false;
	private volatile boolean running = false;
	private volatile boolean log = true;
 /*  
	public HttpServiceTransport(IRegistryClient registryClient, int scaleout) { 
		// create instance of HttpServiceTransport with RegistryClient. The assumption
		// is that the implementation of the client has been fully configured with
		// registry URI and target id. We just pass in a NoOpTarget instead of null.
		// The initialize() will use registry to lookup the client TargetURI and will
		// create correct instance of ITargetURI based on what registry returns
//		this(new NoOpTargetURI(), registryClient, scaleout);
		this(new NoOpTargetURI(), registryClient, scaleout);
		
	}
	*/
//	private HttpServiceTransport(ITargetURI targetUrl, IRegistryClient registryClient, int scaleout) {
	public HttpServiceTransport(IRegistryClient registryClient, int scaleout) throws ServiceException {
		//TargetURIFactory.newTarget(registryClient.lookUp(new NoOpTargetURI().asString()));
		/*
		if ( registryClient == null ) {  
			// the default client just returns the same targetUrl
			// No lookups
			this.registryClient = new DefaultRegistryClient(targetUrl);
		} else {
			this.registryClient = registryClient;
		}
		*/
		this.registryClient = registryClient;
		clientMaxConnections = scaleout;

		try {
			nodeIP = InetAddress.getLocalHost().getHostAddress();
			nodeName=InetAddress.getLocalHost().getCanonicalHostName();
			pid = getProcessIP(NA);
		} catch( UnknownHostException e) {
			throw new RuntimeException(new TransportException("HttpServiceTransport.ctor - Unable to determine Host Name and IP",e));
		}
		
	}
	private HttpPost getPostMethodForCurrentThread() {
		HttpPost postMethod;
		if ( !httpPostMap.containsKey(Thread.currentThread().getId())) {
			// each thread needs its own PostMethod
			postMethod =
			    new HttpPost(currentTargetUrl.asString());
			httpPostMap.put(Thread.currentThread().getId(),postMethod);
		} else {
			postMethod = httpPostMap.get(Thread.currentThread().getId());
		}
		return postMethod;
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
	private void lookupNewTarget() {
		registryLookupLock.lock();
		while( !stopping ) {
			try {
				String newTarget = registryClient.lookUp(currentTargetUrl.asString());
			//	logger.log(Level.INFO, "Registry lookup succesfull - current target URL:"+newTarget);
				currentTargetUrl = TargetURIFactory.newTarget(newTarget);
				break;
			} catch(  Exception e) {
				synchronized (httpClient) {
					
					try {
						httpClient.wait(threadSleepTime);
					} catch( InterruptedException ex) {
						Thread.currentThread().interrupt();
						break;
					}
				}
			}
		}
		if (registryLookupLock.isHeldByCurrentThread()) {
			registryLookupLock.unlock();
		}
	}
	public void addRequestorInfo(IMetaTaskTransaction transaction) {
    	transaction.setRequesterAddress(nodeIP);
    	transaction.setRequesterNodeName(nodeName);
    	transaction.setRequesterProcessId(Integer.valueOf(pid));
    	transaction.setRequesterThreadId((int)Thread.currentThread().getId());
    	if ( logger.isLoggable(Level.FINE )) {
        	logger.log(Level.FINE,"ip:"+transaction.getRequesterAddress());
        	logger.log(Level.FINE, "nodeName:"+transaction.getRequesterNodeName());
        	logger.log(Level.FINE, "processName:"+transaction.getRequesterProcessName());
        	logger.log(Level.FINE,"processId:"+transaction.getRequesterProcessId());
        	logger.log(Level.FINE, "threadId:"+transaction.getRequesterThreadId());

    	}
//
// 		transaction.setRequesterNodeName(nodeName);
// 		transaction.setRequesterProcessId(pid);
// 		transaction.setRequesterProcessName(value);
// 		transaction.setRequesterThreadId((int)Thread.currentThread().getId());
 		
	}
	public void initialize() throws ServiceInitializationException { 
		
		// use plugged in registry to lookup target to connect to.
		// Sets global: currentTarget
		lookupNewTarget();
		
		cMgr = new PoolingHttpClientConnectionManager();

		if (clientMaxConnections > 0) {
			cMgr.setMaxTotal(clientMaxConnections);
		}
		// Set default max connections per route
		if (clientMaxConnectionsPerRoute > 0) {
			cMgr.setDefaultMaxPerRoute(clientMaxConnectionsPerRoute);
		}
		HttpHost httpHost = new HttpHost(currentTargetUrl.asString(), Integer.valueOf(currentTargetUrl.getPort()), currentTargetUrl.getContext());
		if (clientMaxConnectionsPerHostPort > 0) {
			cMgr.setMaxPerRoute(new HttpRoute(httpHost), clientMaxConnectionsPerHostPort);
		}
		
		int timeout = 30;
//		SocketConfig socketConfig = SocketConfig.custom().setSoTimeout(timeout*1000).build();
 //       RequestConfig requestConfig = RequestConfig.custom()
   //     		  .setConnectTimeout(timeout * 1000)
     //   		  .setConnectionRequestTimeout(timeout * 1000)
       // 		  .setSocketTimeout(0).build();
  //      cMgr.setDefaultSocketConfig(socketConfig);
        
//        System.out.println("HttpTransport Max Connections:"+cMgr.getMaxTotal());
//        httpClient = HttpClients.custom().
  //      		setConnectionManager(cMgr).
    //    		setDefaultRequestConfig(requestConfig).build();
        
		httpClient = HttpClients.custom().setConnectionManager(cMgr).build();
		if ( logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO,"Cmgr SoTimeout="+cMgr.getDefaultSocketConfig().getSoTimeout());
		}
		running = true;

	}
    private void addCommonHeaders( HttpPost method ) {
    	synchronized( HttpServiceTransport.class ) {
    		
    		 method.setHeader("IP", nodeIP);
             method.setHeader("Hostname", nodeName);
             method.setHeader("ThreadID",
                             String.valueOf(Thread.currentThread().getId()));
             method.setHeader("PID", pid);
    		
    	}
		
    }

	private HttpEntity wrapRequest(String serializedRequest) {
		return new StringEntity(serializedRequest, ContentType.APPLICATION_XML);
	}

	private boolean isRunning() {
		return running;
	}

	private String retryUntilSuccessfull(String request, HttpPost postMethod) {
		String response="";
		// Only one thread attempts recovery. Other threads will block here
		// until connection to the remote is restored.
		lock.lock();

		// retry until service is stopped
		while (isRunning()) {
			try {
				//response = dispatch(request);
				response =  doPost(postMethod);
				// success, so release the lock so that other waiting threads
				// can retry command
				if (lock.isHeldByCurrentThread()) {
					lock.unlock();
				}

				break;

			} catch (TransportException | IOException | URISyntaxException exx) {
				// Connection still not available so sleep awhile
				synchronized (httpClient) {
					try {
						httpClient.wait(threadSleepTime);
					} catch( InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				lookupNewTarget();

			}
		}
		return response;
		
	}
	private String doPost(HttpPost postMethod) throws URISyntaxException, NoHttpResponseException, IOException, TransportException {
		postMethod.setURI(new URI(currentTargetUrl.asString()));
		HttpResponse response = httpClient.execute(postMethod);
		
		if ( stopping ) {
			throw new TransportException("Service stopping - rejecting request");
		}
		HttpEntity entity = response.getEntity();
		String serializedResponse = EntityUtils.toString(entity);
		StatusLine statusLine = response.getStatusLine();
		if (statusLine.getStatusCode() != 200 && logger.isLoggable(Level.WARNING) ) {
			
			 logger.log(Level.WARNING,"execute", "Unable to Communicate with client - Error:"+statusLine);
			 logger.log(Level.WARNING, "Content causing error:"+serializedResponse);
			throw new TransportException(
					"Http Client Unable to Communicate with a remote client - Error:" + statusLine);
		}
		
		stats.incrementSuccessCount();
		return serializedResponse;
	}
	@Override
	public String dispatch(String serializedRequest) throws TransportException  {
//		System.out.println(".... in dispatch()...stopping="+stopping);
		if ( stopping ) {
			throw new IllegalStateException("Service transport has been stopped, unable to dispatch request");
		}
		HttpEntity e = wrapRequest(serializedRequest);
		// Each thread has its own HttpPost method. If current thread
		// doesnt have one, it will be created and added to the local
		// Map. Subsequent requests will fetch it from the map using
		// current thread ID as a key.
		HttpPost postMethod = getPostMethodForCurrentThread();
		addCommonHeaders(postMethod);
		postMethod.setEntity(e);
		String serializedResponse = null;
		try {
			serializedResponse = doPost(postMethod);
			
		} catch ( NoHttpResponseException ex ) {
			if ( stopping ) {
				System.out.println("Process Thread:"+Thread.currentThread().getId()+" NoHttpResponseException ");
				//ex.printStackTrace();
				throw new TransportException(ex);
			} else {
				serializedResponse = retryUntilSuccessfull(serializedRequest, postMethod);
			}

			// timeout so try again
			//ex.printStackTrace();
		} catch (HttpHostConnectException | UnknownHostException ex ) {
			if ( stopping ) {
				System.out.println("Process Thread:"+Thread.currentThread().getId()+" HttpHostConnectException ");
				
				throw new TransportException(ex);
			}

			stats.incrementErrorCount();
			Action action = handleConnectionError(ex);
			if ( Action.CONTINUE.equals(action)) {
				try {
					// Lost connection to the Task Allocation App
					// Block until connection is restored
					if ( log ) {
						log = false;

						logger.log(Level.INFO, ">>>>>>>>>> Unable to connect to target:"+currentTargetUrl.asString()+" - retrying until successfull - with "+threadSleepTime/1000+" seconds wait between retries  ");
					}
					serializedResponse = retryUntilSuccessfull(serializedRequest, postMethod);
					log = true;
					logger.log(Level.INFO, "Established connection to target:"+currentTargetUrl.asString());
					
				} catch( Exception ee) {
					log = true;
					// Fail here - bad URI
				}
				
				
			} else if ( Action.TERMINATE.equals(action)) {
				ex.printStackTrace();
			}
			
		} catch (SocketException ex) {
			if ( stopping ) {
				//System.out.println("Process Thread:"+Thread.currentThread().getId()+" SocketException ");
				throw new TransportException(ex);
			}
			
		}  catch (TransportException ex) {
			
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new TransportException(ex);
		}
		finally {
			postMethod.releaseConnection();
		}
		return serializedResponse;
		
	}
	
 	private Action handleConnectionError(Exception e) {
 		if ( e instanceof HttpHostConnectException || e instanceof  UnknownHostException  ) {
 			synchronized (httpClient) {
				try {
					httpClient.wait(threadSleepTime);
				} catch( InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
 			return Action.CONTINUE;
 		} else {
 			return Action.TERMINATE;
 		}
 		
 	}
	public void stop() {

		stopping = true;
		running = false;
		//initLatch.countDown();
		logger.log(Level.INFO,this.getClass().getName()+" stop() called");
		if ( cMgr != null ) {
			cMgr.shutdown();
		}
		logger.log(Level.INFO,this.getClass().getName()+" stopped connection mgr");
	}
	public static void main(String[] args) {

	}



}
