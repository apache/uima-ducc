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
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.ducc.user.dd.DeploymentDescriptorGenerator;
import org.apache.uima.ducc.user.dd.DuccUimaAggregate;
import org.apache.uima.ducc.user.dd.DuccUimaAggregateComponent;
import org.apache.uima.ducc.user.dd.IDuccUimaAggregateComponent;
import org.apache.uima.ducc.user.dd.IDuccUimaDeployableConfiguration;
import org.apache.uima.ducc.user.jd.JdUserCollectionReader;
import org.apache.uima.ducc.user.jd.JdUserException;
import org.apache.uima.ducc.user.jd.JdUserMetaCas;
import org.apache.uima.ducc.user.jd.iface.IJdUserDirective;
import org.apache.uima.ducc.user.jd.iface.IJdUserErrorHandler;
import org.apache.uima.ducc.user.jd.iface.JdUserErrorHandler;
import org.apache.uima.ducc.user.jd.iface.JdUserErrorHandler.InitializationDataKey;
import org.apache.uima.ducc.user.jd.test.helper.TestJdUserErrorHandler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSuite {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	private boolean debug = true;
	
	private void out_println(String message) {
		System.out.println(message);
	}
	
	private void debug(String message) {
		if(debug) {
			out_println(message);
		}
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
	
	@Test
	public void test02() {
		try {
			String message = "XYZ";
			JdUserException jdUserException = new JdUserException(message);
			assertTrue(message.equals(jdUserException.getMessage()));
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
				jdUserMetaCas.printMe();
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
			IJdUserErrorHandler eh = new JdUserErrorHandler();
			String serializedCAS = null;
			Exception exception = null;
			IJdUserDirective directive = eh.handle(serializedCAS, exception);
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
			Exception exception = null;
			JdUserErrorHandler eh = null;
			IJdUserDirective directive = null;
			String plist = null;
			int limit = 0;
			//
			exception = null;
			eh = new JdUserErrorHandler();
			directive = eh.handle(serializedCAS, exception);
			assertTrue(directive.isKillJob() == false);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
			//
			exception = new RuntimeException();
			eh = new JdUserErrorHandler();
			directive = eh.handle(serializedCAS, exception);
			assertTrue(directive.isKillJob() == false);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
			//
			limit = 15;
			exception = new RuntimeException();
			eh = new JdUserErrorHandler();
			directive = eh.handle(serializedCAS, exception);
			for(int i=1; i<limit; i++) {
				directive = eh.handle(serializedCAS, exception);
				assertTrue(directive.isKillJob() == false);
				assertTrue(directive.isKillProcess() == false);
				assertTrue(directive.isKillWorkItem() == true);
			}
			directive = eh.handle(serializedCAS, exception);
			assertTrue(directive.isKillJob() == true);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
			//
			limit = 10;
			exception = new RuntimeException();
			plist = InitializationDataKey.KillJobLimit.name()+"="+limit;
			eh = new JdUserErrorHandler(plist);
			directive = eh.handle(serializedCAS, exception);
			for(int i=1; i<limit; i++) {
				directive = eh.handle(serializedCAS, exception);
				assertTrue(directive.isKillJob() == false);
				assertTrue(directive.isKillProcess() == false);
				assertTrue(directive.isKillWorkItem() == true);
			}
			directive = eh.handle(serializedCAS, exception);
			assertTrue(directive.isKillJob() == true);
			assertTrue(directive.isKillProcess() == false);
			assertTrue(directive.isKillWorkItem() == true);
			//
			limit = 20;
			exception = new RuntimeException();
			plist = InitializationDataKey.KillJobLimit.name()+"="+limit;
			eh = new JdUserErrorHandler(plist);
			directive = eh.handle(serializedCAS, exception);
			for(int i=1; i<limit; i++) {
				directive = eh.handle(serializedCAS, exception);
				assertTrue(directive.isKillJob() == false);
				assertTrue(directive.isKillProcess() == false);
				assertTrue(directive.isKillWorkItem() == true);
			}
			directive = eh.handle(serializedCAS, exception);
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
			Exception exception = null;
			TestJdUserErrorHandler eh = null;
			IJdUserDirective directive = null;
			//
			exception = null;
			eh = new TestJdUserErrorHandler();
			directive = eh.handle(serializedCAS, exception);
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
			Exception exception = new RuntimeException("exception text");
			JdUserMetaCas jdUserMetaCas = new JdUserMetaCas(seqNo, serializedCas, documentText, exception);
			assertTrue(seqNo == jdUserMetaCas.getSeqNo());
			assertTrue(serializedCas.equals(jdUserMetaCas.getSerializedCas()));
			assertTrue(documentText.equals(jdUserMetaCas.getDocumentText()));
			assertTrue(exception.equals(jdUserMetaCas.getException()));
			jdUserMetaCas.printMe();
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
			e.printStackTrace();
		}
	}
	
	private IDuccUimaDeployableConfiguration getIDuccUimaDeployableConfiguration() {
		String ddName = "name";
		String ddDescription = "description";
		int ddThreadCount = 1;
		String ddBrokerURL = "brokerURL";
		String ddEndpoint = "endpoint";
		ArrayList<IDuccUimaAggregateComponent> ddComponents = new ArrayList<IDuccUimaAggregateComponent>();
		URL url = this.getClass().getResource("/CR100.xml");
		File file = new File(url.getFile());
		String aeDescriptor = file.getAbsolutePath();
		List<String> aeOverrides = null;
		DuccUimaAggregateComponent aeComponent = new DuccUimaAggregateComponent(aeDescriptor, aeOverrides);
		ddComponents.add(aeComponent);
		IDuccUimaDeployableConfiguration configuration = new DuccUimaAggregate(ddName, ddDescription, ddThreadCount, ddBrokerURL, ddEndpoint, ddComponents);
		return configuration;
	}
	
	private void show(String name) {
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
	public void test10() {
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
			DeploymentDescriptorGenerator ddg = new DeploymentDescriptorGenerator(working.getAbsolutePath());
			IDuccUimaDeployableConfiguration configuration = getIDuccUimaDeployableConfiguration();
			String jobId = "12345";
			String dd = ddg.generate(configuration, jobId);
			debug(dd);
			show(dd);
			delete(working);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
}
