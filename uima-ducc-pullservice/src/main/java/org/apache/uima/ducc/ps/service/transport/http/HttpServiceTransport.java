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
import java.io.InvalidClassException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
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
import org.apache.uima.ducc.ps.service.protocol.INoTaskAvailableStrategy;
import org.apache.uima.ducc.ps.service.registry.IRegistryClient;
import org.apache.uima.ducc.ps.service.transport.IServiceTransport;
import org.apache.uima.ducc.ps.service.transport.ITargetURI;
import org.apache.uima.ducc.ps.service.transport.TransportException;
import org.apache.uima.ducc.ps.service.transport.TransportStats;
import org.apache.uima.ducc.ps.service.transport.XStreamUtils;
import org.apache.uima.ducc.ps.service.transport.target.NoOpTargetURI;
import org.apache.uima.ducc.ps.service.transport.target.TargetURIFactory;
import org.apache.uima.ducc.ps.service.utils.Utils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class HttpServiceTransport implements IServiceTransport {
	private Logger logger =  UIMAFramework.getLogger(HttpServiceTransport.class);
	private HttpClient httpClient = null;
	private PoolingHttpClientConnectionManager cMgr = null;
	private int clientMaxConnections = 1;
	private int clientMaxConnectionsPerRoute = 1;
	private int clientMaxConnectionsPerHostPort = 0;
	private ReentrantLock lock = new ReentrantLock();
	private ReentrantLock registryLookupLock = new ReentrantLock();
    private long threadSleepTime=1000; // millis
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
	private INoTaskAvailableStrategy waitStrategy=null;
	
 	public HttpServiceTransport(IRegistryClient registryClient, int scaleout, INoTaskAvailableStrategy waitStrategy) throws ServiceException {
		this.registryClient = registryClient;
		clientMaxConnections = scaleout;
		this.waitStrategy = waitStrategy;
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
		
		httpClient = HttpClients.custom().setConnectionManager(cMgr).build();
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

	private IMetaTaskTransaction retryUntilSuccessfull(String request, HttpPost postMethod) {
		IMetaTaskTransaction response=null;
		// Only one thread attempts recovery. Other threads will block here
		// until connection to the remote is restored.
		lock.lock();

		// retry until service is stopped
		while (isRunning()) {
			try {
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
	private synchronized IMetaTaskTransaction doPost(HttpPost postMethod) throws URISyntaxException, IOException, TransportException {
		postMethod.setURI(new URI(currentTargetUrl.asString()));
		
		IMetaTaskTransaction metaTransaction=null;
		while(running) {
			HttpResponse response = httpClient.execute(postMethod);
			if ( stopping ) {
				throw new TransportException("Service stopping - rejecting request");
			}
			HttpEntity entity = response.getEntity();
			String serializedResponse = EntityUtils.toString(entity);
			Object transaction=null;
			try {
				transaction = XStreamUtils.unmarshall(serializedResponse);
			} catch(Exception e) {
				logger.log(Level.WARNING,"Process Thread:"+Thread.currentThread().getId()+" Error while deserializing response with XStream",e);
				throw new TransportException(e);
			}
			if (transaction instanceof IMetaTaskTransaction) {
				metaTransaction = (IMetaTaskTransaction) transaction;
				if ( metaTransaction.getMetaTask() == null || metaTransaction.getMetaTask().getUserSpaceTask() == null) {
					logger.log(Level.INFO,"Process Thread:"+Thread.currentThread().getId()+" - Driver is out of tasks - waiting for awhile ("+threadSleepTime+" ms) and will try again ");
					waitStrategy.handleNoTaskSupplied();
				} else {
					// Got a task
					break;
				}
			} else {
				if ( Objects.isNull(transaction)) {
					throw new InvalidClassException(
							"Expected IMetaTaskTransaction - Instead Received NULL");
					
				} else {
					throw new InvalidClassException(
							"Expected IMetaTaskTransaction - Instead Received " + transaction.getClass().getName());
				}
			}
			StatusLine statusLine = response.getStatusLine();
			if (statusLine.getStatusCode() != 200 ) {
				// all IOExceptions are retried
				throw new IOException(
						"Unexpected HttpClient response status:"+statusLine+ " Content causing error:"+serializedResponse);
			}
			
			
		}
		stats.incrementSuccessCount();
		return metaTransaction;
	}
	@Override
	public IMetaTaskTransaction dispatch(String serializedRequest) throws TransportException  {
		if ( stopping ) {
			throw new IllegalStateException("Service transport has been stopped, unable to dispatch request");
		}
		IMetaTaskTransaction transaction=null;
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
			String simulatedException;
			// To test transport errors add command line option -DMockHttpPostError=exception where
			// exception is one of the following Strings:
			//
			// IOException, 
			// SocketException, 
			// UnknownHostException, 
			// NoRouteToHostException,
			// NoHttpResponseException, 
			// HttpHostConnectException, 
			// URISyntaxException
			// Use JUnit test JunitTransoirtTestCase to test the above errors
			
			if ( ( simulatedException = System.getProperty("MockHttpPostError")) != null ) {
				HttpClientExceptionGenerator mockExceptionGenerator = 
						new HttpClientExceptionGenerator(simulatedException);
				mockExceptionGenerator.throwSimulatedException();
			} else {
				transaction = doPost(postMethod);
			}
		} catch( IOException | URISyntaxException ex) {
			if ( stopping ) {
				// looks like the process is in the shutdown mode. Log an exception and dont retry
				logger.log(Level.INFO,"Process Thread:"+Thread.currentThread().getId()+" - Process is already stopping - Caught Exception while calling doPost() \n"+ex);
				throw new TransportException(ex);
			} else {
				if ( log ) {
					log = false;
					stats.incrementErrorCount();
					logger.log(Level.WARNING, this.getClass().getName()+".dispatch() >>>>>>>>>> Handling Exception \n"+ex);
					logger.log(Level.INFO, ">>>>>>>>>> Unable to communicate with target:"+currentTargetUrl.asString()+" - retrying until successfull - with "+threadSleepTime/1000+" seconds wait between retries  ");
				}
				transaction = retryUntilSuccessfull(serializedRequest, postMethod);
				log = true;
				logger.log(Level.INFO, "Established connection to target:"+currentTargetUrl.asString());
			}


		} finally {
			postMethod.releaseConnection();
		}
		return transaction;
		
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
	public void stop(boolean quiesce) {

		stopping = true;
		running = false;
		// Use System.out since the logger's ShutdownHook may have closed streams
		System.out.println(Utils.getTimestamp()+">>>>>>> "+Utils.getShortClassname(this.getClass())+" stop() called - mode:"+(quiesce==true?"quiesce":"stop"));
		logger.log(Level.INFO,this.getClass().getName()+" stop() called");
		if ( !quiesce && cMgr != null ) {
			cMgr.shutdown();
			System.out.println(Utils.getTimestamp()+">>>>>>> "+Utils.getShortClassname(this.getClass())+" stopped connection mgr");
			logger.log(Level.INFO,this.getClass().getName()+" stopped connection mgr");

		}
	}
	public static void main(String[] args) {

	}

	public static class HttpClientExceptionGenerator {
		public enum ERROR{ IOException, SocketException, UnknownHostException, NoRouteToHostException,NoHttpResponseException, HttpHostConnectException, URISyntaxException};
		
		Exception exceptionClass=null;
		
		public HttpClientExceptionGenerator(String exc) {
			
			for( ERROR e : ERROR.values()) {
				if ( exc != null && e.name().equals(exc)) {
					switch(e) {
					case IOException:
						exceptionClass = new IOException("Simulated IOException");
						break;
					case URISyntaxException:
						exceptionClass = new URISyntaxException("", "Simulated URISyntaxException");
						break;
					case NoRouteToHostException:
						exceptionClass = new NoRouteToHostException("Simulated NoRouteToHostException");
						break;
					case NoHttpResponseException:
						exceptionClass = new NoHttpResponseException("Simulated NoHttpResponseException");
						break;	
					case SocketException:
						exceptionClass = new SocketException("Simulated SocketException");
						break;
					case UnknownHostException:
						exceptionClass = new UnknownHostException("Simulated UnknownHostException");
						break;
						
					default:
						
							
					}
					if ( exceptionClass != null ) {
						break;
					}
				}
			}
		}
		public void throwSimulatedException() throws IOException, URISyntaxException {
			if ( exceptionClass != null ) {
				if ( exceptionClass instanceof IOException ) {
					throw (IOException)exceptionClass;
				} else if ( exceptionClass instanceof URISyntaxException ) {
					throw (URISyntaxException)exceptionClass;
				}
				
			}
		}
		
		
	}

}
