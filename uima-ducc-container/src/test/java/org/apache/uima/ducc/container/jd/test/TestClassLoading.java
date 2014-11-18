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
package org.apache.uima.ducc.container.jd.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;

import org.apache.uima.ducc.common.jd.JdFlagsHelper;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverCollectionReader;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverDirective;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverErrorHandler;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.apache.uima.ducc.container.net.impl.MetaCas;
import org.junit.Test;

public class TestClassLoading extends ATest {

	@Test
	public void test_01() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(JdFlagsHelper.Name.UserClasspath.name(), userClasspath);
			ProxyJobDriverErrorHandler pjdeh = new ProxyJobDriverErrorHandler();
			Object serializedCAS = null;
			Object exception = null;
			pjdeh.handle(serializedCAS, exception);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

	@Test
	public void test_02() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(JdFlagsHelper.Name.UserClasspath.name(), userClasspath);
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			System.setProperty(JdFlagsHelper.Name.CollectionReaderXml.name(), crXml);
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
			MetaCas mc = pjdcr.getMetaCas();
			Object serializedCAS = mc.getSerializedCas();
			Object exception = null;
			ProxyJobDriverErrorHandler pjdeh = new ProxyJobDriverErrorHandler();
			pjdeh.handle(serializedCAS, exception);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_03() {
		if(isDisabled(this.getClass().getName())) {
			return;
		}
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(JdFlagsHelper.Name.UserClasspath.name(), userClasspath);
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			System.setProperty(JdFlagsHelper.Name.CollectionReaderXml.name(), crXml);
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
			MetaCas mc = pjdcr.getMetaCas();
			Object serializedCAS = mc.getSerializedCas();
			Object exception = null;
			String className = "org.apache.uima.ducc.user.jd.test.helper.TestJdContainerErrorHandler";
			System.setProperty(JdFlagsHelper.Name.UserErrorHandlerClassname.name(), className);
			ProxyJobDriverErrorHandler pjdeh = new ProxyJobDriverErrorHandler();
			ProxyJobDriverDirective directive = pjdeh.handle(serializedCAS, exception);
			assertTrue(directive.isKillJob() == true);
			assertTrue(directive.isKillProcess() == true);
			assertTrue(directive.isKillWorkItem() == false);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
}
