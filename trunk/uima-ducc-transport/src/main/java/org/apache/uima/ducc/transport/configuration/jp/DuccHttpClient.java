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
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.http.impl.pool.BasicConnPool;
import org.apache.http.util.EntityUtils;
import org.apache.uima.ducc.common.IDuccUser;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.container.sd.ServiceRegistry;
import org.apache.uima.ducc.container.sd.ServiceRegistry_impl;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Direction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Type;
import org.apache.uima.ducc.ps.net.iface.IPerformanceMetrics;
import org.apache.uima.ducc.ps.net.impl.MetaTaskTransaction;
import org.apache.uima.ducc.ps.net.impl.PerformanceMetrics;
import org.apache.uima.ducc.ps.net.impl.TransactionId;

public class DuccHttpClient {
  private final static String REGISTERED_DRIVER = "ducc.deploy.registered.driver";
  private final static String SERVICE_TYPE      = "ducc.deploy.service.type";
  
	private DuccLogger logger = new DuccLogger(DuccHttpClient.class);
	private JobProcessComponent duccComponent;
	private BasicConnPool connPool = null;
	private HttpHost host = null;
	private NodeIdentity nodeIdentity;
	private String pid = "";
	private ReentrantLock lock = new ReentrantLock();
	private HttpClient httpClient = null;
	private String jdUrl;
	private PoolingHttpClientConnectionManager cMgr = null;

	private int ClientMaxConnections = 0;
	private int ClientMaxConnectionsPerRoute = 0;
	private int ClientMaxConnectionsPerHostPort = 0;
  private ServiceRegistry registry = null;
  private String taskServerName;
	
  public DuccHttpClient(JobProcessComponent duccComponent) {
    this.duccComponent = duccComponent;
  }
  
	public void setScaleout(int scaleout) {
		connPool.setMaxTotal(scaleout);
		connPool.setDefaultMaxPerRoute(scaleout);
		connPool.setMaxPerRoute(host, scaleout);
	}
	
	// If no registry use the url in the system properties, e.g. JD/JP case
	// The fetch should return one of the values saved by the most recent notification.
	// but should block if no instance currently registered.
	public String getJdUrl() {
	  if (registry == null) {
	    return jdUrl;
	  }
	  String address = registry.fetch(taskServerName);   // Will block if none registered
	  logger.info("getJdUrl", null, "Registry entry for", taskServerName, "is", address);
	  return address;
	}
	
	// If the client URL provided use it (JD/JP case)
	// Otherwise in the registry lookup "ducc.deploy.registered.driver"
	// which must specify: <registry-location>?<registry-entry>
	public void initialize(String jdUrl) throws Exception {

	  // If not specified get the url from the registry
	  if (jdUrl == null || jdUrl.isEmpty()) {
	    String registryAddr = null;
	    String registryUri = System.getProperty(REGISTERED_DRIVER);
	    if (registryUri != null) {
	      String[] parts = registryUri.split("\\?", 2);
	      if (parts.length == 2) {
	        registryAddr = parts[0];
	        taskServerName = parts[1];
	      }
	    }
	    if (registryAddr == null) {
	      throw new RuntimeException("Missing or invalid system property " + REGISTERED_DRIVER + ": " + registryUri);
      }
	    registry = ServiceRegistry_impl.getInstance();
	    if (!registry.initialize(registryAddr)) {
	      throw new RuntimeException("Failed to connect to registry at "+registryAddr+" to locate server "+taskServerName);
	    }
	    logger.info("initialize", null, "Using registry at", registryAddr, "to locate server", taskServerName);
	    jdUrl = getJdUrl();
	  }
	  this.jdUrl = jdUrl;
		
		logger.info("initialize", null, "Found jdUrl =", jdUrl);
		
		int pos = jdUrl.indexOf("//");
        int ipEndPos = jdUrl.indexOf(":", pos);
        String jdIP = jdUrl.substring(pos+2,ipEndPos);
        int portEndPos = jdUrl.indexOf("/", ipEndPos);
        String jdScheme = jdUrl.substring(portEndPos+1);
        String jdPort = jdUrl.substring(ipEndPos+1, portEndPos);

		
		pid = getProcessIP("N/A");
		nodeIdentity = new NodeIdentity();
		cMgr = new PoolingHttpClientConnectionManager();
		
        if(ClientMaxConnections > 0) {
            cMgr.setMaxTotal(ClientMaxConnections);
        }
        // Set default max connections per route                                                                                                                   
        if(ClientMaxConnectionsPerRoute > 0) {
            cMgr.setDefaultMaxPerRoute(ClientMaxConnectionsPerRoute);
        }
		
		// Set max connections for host:port                                                                                                                       
        
        HttpHost httpHost = new HttpHost(jdIP, Integer.valueOf(jdPort),jdScheme);
        if(ClientMaxConnectionsPerHostPort > 0) {
          cMgr.setMaxPerRoute(new HttpRoute(httpHost), ClientMaxConnectionsPerHostPort);
        }

        httpClient = HttpClients.custom().setConnectionManager(cMgr).build();

	}
	public void stop() throws Exception {
		if ( cMgr != null ) {
			cMgr.shutdown();
		}
	}
//	public void testConnection() throws Exception {
//		// test connection to the JD
//	    Future<BasicPoolEntry> future = connPool.lease(host,  null);
//		BasicPoolEntry poolEntry = null;
//		try {
//			poolEntry= future.get();
//		} finally {
//			connPool.release(poolEntry, true);
//		}
//	}
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
		if ( System.getenv(IDuccUser.EnvironmentVariable.DUCC_IP.value()) != null) {
			ip = System.getenv(IDuccUser.EnvironmentVariable.DUCC_IP.value());
		}
		return ip;
	}
	private String getNodeName() {
		String nn =nodeIdentity.getCanonicalName();
		if ( System.getenv(IDuccUser.EnvironmentVariable.DUCC_NODENAME.value()) != null) {
			nn = System.getenv(IDuccUser.EnvironmentVariable.DUCC_NODENAME.value());
		}
		return nn;
	}
	private String getProcessName() {
	  String pn = System.getProperty(SERVICE_TYPE);  // Indicates the type of service request
	  if (pn == null) {   // JP's use the ID set by the agent
	    pn = System.getenv(IDuccUser.EnvironmentVariable.DUCC_ID_PROCESS.value());
	  }
		return pn;
	}
	
    private void addCommonHeaders( IMetaTaskTransaction transaction ) {
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
    
    private void addCommonHeaders( HttpPost method ) {
    	synchronized( DuccHttpClient.class) {
    		
    		 method.setHeader("IP", getIP());
             method.setHeader("Hostname", getNodeName());
             method.setHeader("ThreadID",
                             String.valueOf(Thread.currentThread().getId()));
             method.setHeader("PID", pid);
    		
    	}
		
    }

	public IMetaTaskTransaction execute( IMetaTaskTransaction transaction, HttpPost postMethod ) throws Exception {
		Exception lastError = null;
		IMetaTaskTransaction reply=null;

		addCommonHeaders(transaction);
		transaction.setDirection(Direction.Request);
		
		try {
				// Serialize request object to XML
				String body = XStreamUtils.marshall(transaction);
	            HttpEntity e = new StringEntity(body,ContentType.APPLICATION_XML); //, "application/xml","UTF-8" );
	         
	            postMethod.setEntity(e);
	            
	            addCommonHeaders(postMethod);
	    
	            logger.debug("execute",null, "calling httpClient.executeMethod()");
	            // wait for a reply. When connection fails, retry indefinitely
	            HttpResponse response = null;
	            try {
	            	 response = httpClient.execute(postMethod);
	            } catch( HttpHostConnectException ex) {
	            	// Lost connection to the Task Allocation App
	            	// Block until connection is restored
	            	response = retryUntilSuccessfull(transaction, postMethod);
	            } catch( NoHttpResponseException ex ) {
	            	// Lost connection to the Task Allocation App
	            	// Block until connection is restored
	            	response = retryUntilSuccessfull(transaction, postMethod);
	            	
	            }
	            // we may have blocked in retryUntilSuccessfull while this process
	            // was told to stop
	            if ( !duccComponent.isRunning() ) {
	            	return null;
	            }
	            logger.debug("execute",null, "httpClient.executeMethod() returned");
	            HttpEntity entity = response.getEntity();
                String content = EntityUtils.toString(entity);
                StatusLine statusLine = response.getStatusLine();
                if ( statusLine.getStatusCode() != 200) {
                    logger.error("execute", null, "Unable to Communicate with JD - Error:"+statusLine);
                    logger.error("execute", null, "Content causing error:"+content);
                    throw new RuntimeException("JP Http Client Unable to Communicate with JD - Error:"+statusLine);
                }
                logger.debug("execute", null, "Thread:"+Thread.currentThread().getId()+" JD Reply Status:"+statusLine);
                logger.debug("execute", null, "Thread:"+Thread.currentThread().getId()+" Recv'd:"+content);

				Object o;
				try {
					o = XStreamUtils.unmarshall(content); //sb.toString());
					
				} catch( Exception ex) {
					logger.error("execute", null, "Thread:"+Thread.currentThread().getId()+" ERRR::Content causing error:"+content,ex);
					throw ex;
				}
				if ( o instanceof IMetaTaskTransaction) {
					reply = (MetaTaskTransaction)o;
				} else {
					throw new InvalidClassException("Expected IMetaCasTransaction - Instead Received "+o.getClass().getName());
				}
		} catch( Exception t) {
			lastError = t;
		}
		finally {
			postMethod.releaseConnection();
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

	private HttpResponse retryUntilSuccessfull(IMetaTaskTransaction transaction, HttpPost postMethod) throws Exception {
        HttpResponse response=null;
		// Only one thread attempts recovery. Other threads will block here
		// until connection to the remote is restored. 
		logger.error("retryUntilSucessfull", null, "Connection Lost to", postMethod.getURI(), "- Retrying Until Successful ...");
   		lock.lock();

         // retry indefinitely
		while( duccComponent.isRunning() ) {
       		try {
       			// retry the command
       		  jdUrl = getJdUrl();
       		  URI jdUri = new URI(jdUrl);
       		  postMethod.setURI(jdUri);
       		  logger.warn("retryUntilSucessfull", null, "Trying to connect to", jdUrl);
       			response = httpClient.execute(postMethod);
       			logger.warn("retryUntilSucessfull", null, "Recovered Connection");

       			// success, so release the lock so that other waiting threads
       			// can retry command
       		    if ( lock.isHeldByCurrentThread()) {
        		    lock.unlock();
    		    }

       			break;
       			
       		} catch( HttpHostConnectException exx ) {
       			// Connection still not available so sleep awhile
       			synchronized(postMethod) {
       			  logger.warn("retryUntilSucessfull", null, "Connection failed - retry in", duccComponent.getThreadSleepTime()/1000, "secs");
       				postMethod.wait(duccComponent.getThreadSleepTime());
       			}
       		}
        }
		return response;
	}

	public static void main(String[] args) {
		try {
			HttpPost postMethod = new HttpPost(args[0]);
			DuccHttpClient client = new DuccHttpClient(null);
		//	client.setScaleout(10);
			//client.setTimeout(30000);
			client.initialize(args[0]);
			int minor = 0;
			IMetaTaskTransaction transaction = new MetaTaskTransaction();
			AtomicInteger seq = new AtomicInteger(0);
			TransactionId tid = new TransactionId(seq.incrementAndGet(), minor);
			transaction.setTransactionId(tid);
			// According to HTTP spec, GET may not contain Body in 
			// HTTP request. HttpClient actually enforces this. So
			// do a POST instead of a GET.
			transaction.setType(Type.Get);  // Tell JD you want a Work Item
			//String command = Type.Get.name();
			// send a request to JD and wait for a reply
	    	transaction = client.execute(transaction, postMethod);
	        // The JD may not provide a Work Item to process.
	    	if ( transaction.getMetaTask()!= null) {
	    		// Confirm receipt of the CAS. 
				transaction.setType(Type.Ack);
				//command = Type.Ack.name();
				tid = new TransactionId(seq.incrementAndGet(), minor++);
				transaction.setTransactionId(tid);
				transaction = client.execute(transaction, postMethod); 

	        }
			transaction.setType(Type.End);
			//command = Type.End.name();
			tid = new TransactionId(seq.incrementAndGet(), minor++);
			transaction.setTransactionId(tid);
		//	IPerformanceMetrics metricsWrapper =
		//			new PerformanceMetrics();
			
			//metricsWrapper.set(Arrays.asList(new Properties()));
			//transaction.getMetaTask().setPerformanceMetrics(metricsWrapper);
			
			transaction = client.execute(transaction, postMethod); 

			
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
	
}
