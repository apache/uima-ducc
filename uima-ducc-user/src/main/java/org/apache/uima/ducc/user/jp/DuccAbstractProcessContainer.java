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
package org.apache.uima.ducc.user.jp;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.CasHelper;
import org.apache.uima.ducc.user.common.DuccUimaSerializer;
import org.apache.uima.ducc.user.common.QuotedOptions;
import org.apache.uima.ducc.user.dgen.iface.DeployableGeneration;
import org.apache.uima.ducc.user.jp.iface.IProcessContainer;
import org.apache.uima.resource.metadata.FsIndexDescription;
import org.apache.uima.resource.metadata.TypePriorities;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;

public abstract class DuccAbstractProcessContainer implements IProcessContainer{
	// Container implementation must implement the following methods
    protected abstract void doDeploy() throws Exception;
    protected abstract int doInitialize(Properties p, String[] arg) throws Exception;
    protected abstract void doStop() throws Exception;
    protected abstract List<Properties>  doProcess(Object subject) throws Exception;
    protected 	AnalysisEngineMetaData analysisEngineMetadata;

    protected int scaleout=1;
    // Map to store DuccUimaSerializer instances. Each has affinity to a thread
	protected static Map<Long, DuccUimaSerializer> serializerMap =
			new HashMap<Long, DuccUimaSerializer>();

	protected final boolean debug = System.getProperty("ducc.debug") != null;

   
	/**
	 * This method is called to fetch a WorkItem ID from a given CAS which
	 * is required to support investment reset. 
	 *
	 */
	public String getKey(String xmi) throws Exception {
		if ( analysisEngineMetadata == null ) {
			// WorkItem ID (key) is only supported for pieces 'n parts 
			return null;
		} 
		Properties props = new Properties();
        props.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, "1000");

		TypeSystemDescription tsd = analysisEngineMetadata.getTypeSystem();
		TypePriorities tp = analysisEngineMetadata.getTypePriorities();
		FsIndexDescription[] fsid = analysisEngineMetadata.getFsIndexes();
		CAS cas;
		synchronized( CasCreationUtils.class) {
			cas = CasCreationUtils.createCas(tsd, tp, fsid, props);
		}
		// deserialize the CAS
		getUimaSerializer().deserializeCasFromXmi((String)xmi, cas);
		
		String key = CasHelper.getId(cas);
		cas.release();
		return key;
	}
    public int getScaleout( ){
		return scaleout;
	}

    protected DuccUimaSerializer getUimaSerializer() {
    	return serializerMap.get(Thread.currentThread().getId());
    }

    public int initialize(Properties p, String[] arg) throws Exception {
    	System.out.println("DuccAbstractProcessContainer.initialize() >>>>>>>>> Initializing User Container");

    	// save current context cl and inject System classloader as
		// a context cl before calling user code. This is done in 
		// user code needs to load resources 
		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
		try {
    		return doInitialize(p, arg);
        }finally {
			Thread.currentThread().setContextClassLoader(savedCL);
 	 		System.out.println("DuccAbstractProcessContainer.initialize() <<<<<<<< User Container initialized");
        }
    }
    public void deploy() throws Exception {

    	System.out.println("DuccAbstractProcessContainer.deploy() >>>>>>>>> Deploying User Container");
    	// save current context cl and inject System classloader as
 		// a context cl before calling user code. 
 		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
 		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
         try {
     		doDeploy();
         } catch( Exception e) {
        	 throw e;
         } finally {
 			Thread.currentThread().setContextClassLoader(savedCL);
 			//	Pin thread to its own CAS serializer instance
 			serializerMap.put( Thread.currentThread().getId(), new DuccUimaSerializer());
			System.out.println("DuccAbstractProcessContainer.deploy() <<<<<<<< User Container deployed");
         }
     }
    public List<Properties> process(Object xmi) throws Exception {
    	if (debug) System.out.println("DuccAbstractProcessContainer.process() >>>>>>>>> Processing User Container");
 		// save current context cl and inject System classloader as
 		// a context cl before calling user code. 
 		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
 		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
         try {
     		return doProcess(xmi);
         }finally {
 			Thread.currentThread().setContextClassLoader(savedCL);
 			if (debug) System.out.println("DuccAbstractProcessContainer.process() <<<<<<<< User Container processed");
         }
     }
    public void stop() throws Exception {
    	if (debug) System.out.println("DuccAbstractProcessContainer.stop() >>>>>>>>> Stopping User Container");
 		// save current context cl and inject System classloader as
 		// a context cl before calling user code. 
 		ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
 		Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
         
 		try {
     		doStop();
         }finally {
 			Thread.currentThread().setContextClassLoader(savedCL);
 	 		if (debug) System.out.println("DuccAbstractProcessContainer.stop() <<<<<<<< User Container stopped");
         }
     }

    protected String serializeAsString(Throwable t) throws Exception {
        StringWriter sw = new StringWriter();
        String serializedCause = "";
        try {
 
            t.printStackTrace(new PrintWriter(sw));
            serializedCause =  sw.toString();
        } catch (Throwable e) {
			e.printStackTrace();
			// Unable to serialize user Exception (not Serializable?)
			// Just send a simple msg telling user to check service log
			serializedCause = "Unable to Serialize User Exception - Please Check JP Log File For More Details";
		}
        return serializedCause;
    }
    protected byte[] serialize(Throwable t) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);

		try {
			oos.writeObject(t);
		} catch (Exception e) {
			e.printStackTrace();
			// Unable to serialize user Exception (not Serializable?)
			// Create a new Exception and serialize it
			RuntimeException re 
 			   = new RuntimeException("Unable to Serialize User Exception - Please Check JP Log File For More Details");
			oos.writeObject(re);
		}
		oos.close();
		return baos.toByteArray();
	}
    private Socket connectWithAgent() throws Exception {
    	InetAddress host = null;
        int statusUpdatePort = -1;
    	
    	host = InetAddress.getLocalHost();
    	String port = System.getenv("DUCC_STATE_UPDATE_PORT");
    	if ( port == null ) {
    	} else {
    		try {
       		   statusUpdatePort = Integer.valueOf(port);
   		    } catch( NumberFormatException nfe) {
    		}
    	}
    	System.out.println("Service Connecting Socket to Host:"+host.getHostName()+" Port:"+statusUpdatePort);
    	String localhost=null;
    	//establish socket connection to an agent where this process will report its state
        return new Socket(localhost, statusUpdatePort);

    }
    protected void sendStateUpdate(String state) throws Exception {
    	DataOutputStream out = null;
    	Socket socket=null;
    	try {

    		StringBuilder sb =
            		new StringBuilder();
            
            sb.append("DUCC_PROCESS_UNIQUEID=").append(System.getenv("DUCC_PROCESS_UNIQUEID")).append(",");
            sb.append("DUCC_PROCESS_STATE=").append(state);
            socket = connectWithAgent();
            out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF(sb.toString());
            out.flush();
            System.out.println("Sent new State:"+state);
    	} catch( Exception e) {
    		throw e;
    	} finally {
    		if ( out != null ) {
        		out.close();
    		}
    		if ( socket != null ) {
    			socket.close();
    		}
    	}
      
    }
	  private boolean dump = false;
	  
	  private void dumpSystemProperties() {
		  if(dump) {
			  System.out.println("===== <System Properties> =====");
			  Properties props = System.getProperties();
			  for(Entry<Object, Object> entry : props.entrySet()) {
				  String key = (String) entry.getKey();
				  String value = (String) entry.getValue();
				  System.out.println(key+"="+value);
			  }
			  System.out.println("===== </System Properties> =====");
		  }
	  }
	  
	  private String getPropertyString(String key) {
		  String value = System.getProperty(key);
		  //System.out.println(key+"="+value);
		  return value;
	  }
	  
	  private List<String> getPropertyListString(String key) {
	    String input = System.getProperty(key);
	    return QuotedOptions.tokenizeList(input, true);
	  }
	  
	  private Integer getPropertyInteger(String key) {
		  String sval = getPropertyString(key);
		  Integer value = new Integer(sval);
		  return value;
	  }
	  
	  // Build just an AE from parts and return the filename
	  // (DD's are converted in UimaAsProcessContainer.parseDD)
	  protected String buildDeployable() {
		  try {
			  dumpSystemProperties();
			  String directory = getPropertyString("ducc.deploy.JobDirectory"); 
			  String id = getPropertyString("ducc.job.id");
			  Integer dgenThreadCount = getPropertyInteger("ducc.deploy.JpThreadCount");
			  String dgenFlowController = getPropertyString("ducc.deploy.JpFlowController");
			  String jpType = getPropertyString("ducc.deploy.JpType");
			  if(jpType == null) {
				  jpType = "uima";
			  }
			  if(jpType.equalsIgnoreCase("uima-as")) {
				  System.out.println("ERROR - should not be called for type="+jpType);
			  }
			  else {
				  String cmDescriptor = getPropertyString("ducc.deploy.JpCmDescriptor"); 
				  List<String> cmOverrides = getPropertyListString("ducc.deploy.JpCmOverrides");
				  String aeDescriptor = getPropertyString("ducc.deploy.JpAeDescriptor"); 
				  List<String> aeOverrides = getPropertyListString("ducc.deploy.JpAeOverrides"); 
				  String ccDescriptor = getPropertyString("ducc.deploy.JpCcDescriptor"); 
				  List<String> ccOverrides = getPropertyListString("ducc.deploy.JpCcOverrides"); 
				  DeployableGeneration dg = new DeployableGeneration();
				  String name = dg.generate(
						  directory, 
						  id, 
						  dgenThreadCount, 
						  dgenFlowController, 
						  cmDescriptor, 
						  cmOverrides, 
						  aeDescriptor, 
						  aeOverrides, 
						  ccDescriptor, 
						  ccOverrides,
						  true                       // create unique temporary file
						  );
				  return name;
			  }
		  }
		  catch(Exception e) {
			  e.printStackTrace();
		  }
		  return null;
	  }
	  
}
