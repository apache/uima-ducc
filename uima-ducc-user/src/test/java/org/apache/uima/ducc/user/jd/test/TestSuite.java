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
package org.apache.uima.ducc.user.jd.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.ErrorHandler;
import org.apache.uima.ducc.ErrorHandler.InitializationDataKey;
import org.apache.uima.ducc.IErrorHandler;
import org.apache.uima.ducc.IErrorHandlerDirective;
import org.apache.uima.ducc.user.common.ExceptionHelper;
import org.apache.uima.ducc.user.dgen.DeployableGenerator;
import org.apache.uima.ducc.user.dgen.DuccUimaAggregate;
import org.apache.uima.ducc.user.dgen.DuccUimaAggregateComponent;
import org.apache.uima.ducc.user.dgen.IDuccGeneratorUimaAggregateComponent;
import org.apache.uima.ducc.user.dgen.IDuccGeneratorUimaDeployableConfiguration;
import org.apache.uima.ducc.user.dgen.iface.DeployableGeneration;
import org.apache.uima.ducc.user.jd.JdUserCollectionReader;
import org.apache.uima.ducc.user.jd.JdUserMetaCas;
import org.apache.uima.ducc.user.jd.test.helper.TestErrorHandler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSuite {

	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		if(!debug) {
			System.setOut(new PrintStream(outContent));
			System.setErr(new PrintStream(errContent));
		}
	}

	@After
	public void tearDown() throws Exception {
		System.setOut(null);
	    System.setErr(null);
	}
	
	private boolean debug = false;
	
	private void out_println(String message) {
		System.out.println(message);
	}
	
	private void debug(String message) {
		if(debug) {
			out_println(message);
		}
	}
	
	private Object getUserException() {
		Object retVal = null;
		return retVal;
	}
	
	@Test
	public void test01() {
		try {
			int seqNo = 1;
			String serializedCas = "ABC";
			String documentText = "123";
			JdUserMetaCas jdUserMetaCas = new JdUserMetaCas(seqNo, serializedCas, documentText);
			assertTrue(seqNo == jdUserMetaCas.getSeqNo());
			assertTrue(serializedCas.equals(jdUserMetaCas.getSerializedCas()));
			assertTrue(documentText.equals(jdUserMetaCas.getDocumentText()));
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@SuppressWarnings("null")
	private Exception getException() {
		Exception retVal = null;
		try {
			retVal.hashCode();
		}
		catch(Exception e) {
			retVal = e;
		}
		return retVal;
	}
	@Test
	public void test02() {
		try {
			Exception e = getException();
			Exception jdUserException = ExceptionHelper.wrapStringifiedException(e);
			String userException = jdUserException.getMessage();
			assertTrue(userException.startsWith("java.lang.NullPointerException"));
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test03() {
		try {
			URL url = this.getClass().getResource("/CR100.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			JdUserCollectionReader jdcr = new JdUserCollectionReader(crXml, crCfg);
			int total = jdcr.getTotal();
			assertTrue(total == 100);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test04() {
		try {
			URL url = this.getClass().getResource("/CR100.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			JdUserCollectionReader jdcr = new JdUserCollectionReader(crXml, crCfg);
			int total = jdcr.getTotal();
			assertTrue(total == 100);
			int counter = 0;
			JdUserMetaCas jdUserMetaCas = jdcr.getJdUserMetaCas();
			while(jdUserMetaCas != null) {
				if(debug) {
					jdUserMetaCas.printMe();
				}
				counter++;
				if(counter > 100) {
					fail("Too many CASes: "+counter);
				}
				jdUserMetaCas = jdcr.getJdUserMetaCas();
			}
			if(counter < 100) {
				fail("Not enough CASes: "+counter);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test05() {
		try {
			URL url = this.getClass().getResource("/CR100.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			JdUserCollectionReader jdcr = new JdUserCollectionReader(crXml, crCfg);
			int total = jdcr.getTotal();
			assertTrue(total == 100);
			JdUserMetaCas jdUserMetaCas = null;
			int counter = 0;
			for(int i=0; i<total; i++) {
				jdUserMetaCas = jdcr.getJdUserMetaCas();
				assertTrue(jdUserMetaCas != null);
				counter++;
			}
			jdUserMetaCas = jdcr.getJdUserMetaCas();
			assertTrue(jdUserMetaCas == null);
			assertTrue(counter == 100);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test06() {
		try {
			IErrorHandler eh = new ErrorHandler();
			String serializedCAS = null;
			IErrorHandlerDirective directive = eh.handle(serializedCAS, getUserException());
			assertTrue(directive.isKillJob() == false);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test07() {
		try {
			URL url = this.getClass().getResource("/CR100.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			JdUserCollectionReader jdcr = new JdUserCollectionReader(crXml, crCfg);
			int total = jdcr.getTotal();
			assertTrue(total == 100);
			JdUserMetaCas jdUserMetaCas = null;
			jdUserMetaCas = jdcr.getJdUserMetaCas();
			assertTrue(jdUserMetaCas != null);
			String serializedCAS = jdUserMetaCas.getSerializedCas();
			assertTrue(serializedCAS != null);
			//
			ErrorHandler eh = null;
			IErrorHandlerDirective directive = null;
			String plist = null;
			int limit = 0;
			//
			eh = new ErrorHandler();
			directive = eh.handle(serializedCAS, getUserException());
			assertTrue(directive.isKillJob() == false);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
			//
			eh = new ErrorHandler();
			directive = eh.handle(serializedCAS, getUserException());
			assertTrue(directive.isKillJob() == false);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
			//
			limit = 15;
			eh = new ErrorHandler();
			directive = eh.handle(serializedCAS, getUserException());
			for(int i=1; i<limit; i++) {
				directive = eh.handle(serializedCAS, getUserException());
				assertTrue(directive.isKillJob() == false);
				assertTrue(directive.isKillProcess() == false);
				assertTrue(directive.isKillWorkItem() == true);
			}
			directive = eh.handle(serializedCAS, getUserException());
			assertTrue(directive.isKillJob() == true);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
			//
			limit = 10;
			plist = InitializationDataKey.KillJobLimit.name()+"="+limit;
			eh = new ErrorHandler(plist);
			directive = eh.handle(serializedCAS, getUserException());
			for(int i=1; i<limit; i++) {
				directive = eh.handle(serializedCAS, getUserException());
				assertTrue(directive.isKillJob() == false);
				assertTrue(directive.isKillProcess() == false);
				assertTrue(directive.isKillWorkItem() == true);
			}
			directive = eh.handle(serializedCAS, getUserException());
			assertTrue(directive.isKillJob() == true);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
			//
			limit = 20;
			plist = InitializationDataKey.KillJobLimit.name()+"="+limit;
			eh = new ErrorHandler(plist);
			directive = eh.handle(serializedCAS, getUserException());
			for(int i=1; i<limit; i++) {
				directive = eh.handle(serializedCAS, getUserException());
				assertTrue(directive.isKillJob() == false);
				assertTrue(directive.isKillProcess() == false);
				assertTrue(directive.isKillWorkItem() == true);
			}
			directive = eh.handle(serializedCAS, getUserException());
			assertTrue(directive.isKillJob() == true);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test08() {
		try {
			//
			String serializedCAS = null;
			TestErrorHandler eh = null;
			IErrorHandlerDirective directive = null;
			//
			eh = new TestErrorHandler();
			directive = eh.handle(serializedCAS, getUserException());
			assertTrue(directive.isKillJob() == true);
			assertTrue(directive.isKillProcess() == true);
			assertTrue(directive.isKillWorkItem() == false);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test09() {
		try {
			int seqNo = 1;
			String serializedCas = "ABC";
			String documentText = "123";
			Exception userException = new RuntimeException("exception text");
			JdUserMetaCas jdUserMetaCas = new JdUserMetaCas(seqNo, serializedCas, documentText, userException);
			assertTrue(seqNo == jdUserMetaCas.getSeqNo());
			assertTrue(serializedCas.equals(jdUserMetaCas.getSerializedCas()));
			assertTrue(documentText.equals(jdUserMetaCas.getDocumentText()));
			assertTrue(userException.equals(jdUserMetaCas.getUserException()));
			if(debug) {
				jdUserMetaCas.printMe();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	private void delete(File directory) {
		try {
			for(File file : directory.listFiles()) {
				debug("delete: "+file.getName());
				file.delete();
			}
			debug("delete: "+directory.getName());
			directory.delete();
		}
		catch(Exception e) {
			//e.printStackTrace();
		}
	}
	
	private IDuccGeneratorUimaDeployableConfiguration getIDuccUimaDeployableConfiguration() {
		String aeName = "name";
		String aeDescription = "description";
		int aeThreadCount = 1;
		String aeBrokerURL = "brokerURL";
		String aeEndpoint = "endpoint";
		String aeFlowController = "flowController";
		ArrayList<IDuccGeneratorUimaAggregateComponent> aeComponents = new ArrayList<IDuccGeneratorUimaAggregateComponent>();
		URL url = this.getClass().getResource("/CR100.xml");
		File file = new File(url.getFile());
		String aeDescriptor = file.getAbsolutePath();
		List<String> aeOverrides = null;
		DuccUimaAggregateComponent aeComponent = new DuccUimaAggregateComponent(aeDescriptor, aeOverrides);
		aeComponents.add(aeComponent);
		IDuccGeneratorUimaDeployableConfiguration configuration = new DuccUimaAggregate(aeName, aeDescription, aeThreadCount, aeBrokerURL, aeEndpoint, aeFlowController, aeComponents);
		return configuration;
	}
	
	protected void show(String name) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(name));
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
			br.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test10a() {
		try {
			URL url = this.getClass().getResource("/");
			File root = new File(url.getFile());
			String name = root.getAbsolutePath();
			debug(name);
			assertTrue(root.isDirectory());
			String nameWorking = name+File.separator+"working";
			File working = new File(nameWorking);
			delete(working);
			working.mkdir();
			DeployableGenerator aeGenerator = new DeployableGenerator(working.getAbsolutePath());
			IDuccGeneratorUimaDeployableConfiguration configuration = getIDuccUimaDeployableConfiguration();
			String jobId = "12345";
			String ae = aeGenerator.generate(configuration, jobId);
			debug(ae);
			//show(ae);
			delete(working);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test10b() {
		try {
			URL url = this.getClass().getResource("/");
			File root = new File(url.getFile());
			String name = root.getAbsolutePath();
			debug(name);
			assertTrue(root.isDirectory());
			String nameWorking = name+File.separator+"working";
			File working = new File(nameWorking);
			delete(working);
			working.mkdir();
			DeployableGeneration dg = new DeployableGeneration();
			
			String directory = working.getAbsolutePath();
			String id = "99999";
			String aeName = "aeName";
			String aeDescription = "aeDescription";
			Integer aeThreadCount = new Integer(11);
			String aeBrokerURL = "aeBrokerURL";
			String aeBrokerEndpoint = "aeBrokerEndpoint";
			String aeFlowController = "aeFlowController";
			String cmDescriptor = null;
			List<String> cmOverrides = null;
			String aeDescriptor = root+File.separator+"FixedSleepAE.xml";
			List<String> aeOverrides = null;
			String ccDescriptor = null;
			List<String> ccOverrides = null;
			
			String ae = dg.generate(
					directory, 
					id, aeName, 
					aeDescription, 
					aeThreadCount, 
					aeBrokerURL, 
					aeBrokerEndpoint, 
					aeFlowController,
					cmDescriptor, 
					cmOverrides, 
					aeDescriptor, 
					aeOverrides, 
					ccDescriptor, 
					ccOverrides);
			
			debug(ae);
			//show(ae);
			delete(working);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test11() {
		try {
			URL url = this.getClass().getResource("/CrInitException.xml");
			File file = new File(url.getFile());
			String crXml = file.getAbsolutePath();
			debug(crXml);
			String crCfg = null;
			new JdUserCollectionReader(crXml, crCfg);
			fail("No Exception?");
		}
		catch(Exception e) {
			String message = e.getMessage();
			//System.out.println(message);
			if(message.startsWith("java.lang.RuntimeException")) {
				// as expected!
			}
			else {
				e.printStackTrace();
				fail("Exception");
			}
		}
	}
}
