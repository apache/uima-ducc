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

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.common.classloader.ProxyException;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverCollectionReader;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.apache.uima.ducc.container.net.impl.MetaCas;

public class TestSuite extends TestBase {
	
	String prefix1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xmi:XMI xmlns:tcas=\"http:///uima/tcas.ecore\"";
	String prefix0 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xmi:XMI xmlns:cas=\"http:///uima/cas.ecore\"";
	 
	private void checkCas(String cas) {
		assertTrue(cas.startsWith(prefix0) || cas.startsWith(prefix1));
	}
	
	protected void config() {
		URL urlXml = this.getClass().getResource("/CR100.xml");
		File file = new File(urlXml.getFile());
		String crXml = file.getAbsolutePath();
		System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), crXml);
		String userClasspath = Utilities.getInstance().getUserCP();
		System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
	}
	
	protected void testIncludeAll() {
		try {
			config();
			new ProxyJobDriverCollectionReader();
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	protected void testExcludeOne(int skip) {
		try {
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), crXml);
			String userClasspath = Utilities.getInstance().getUserCP();
			String[] classpathParts = userClasspath.split(File.pathSeparator);
			StringBuffer sb = new StringBuffer();
			for(int i=0; i<classpathParts.length; i++) {
				String jar = classpathParts[i];
				if(i == skip) {
					debug(i+" skip: "+jar);
				}
				else {
					debug(i+" use: "+jar);
					sb.append(this.getClass().getResource(jar));
					sb.append(File.pathSeparator);
				}
			}
			try {
				String userPartialClasspath = sb.toString();
				System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userPartialClasspath);
				new ProxyJobDriverCollectionReader();
				fail("Exception missing...?");
			}
			catch(ProxyException e) {
				asExpected(e);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	protected void testNoXml() {
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			new ProxyJobDriverCollectionReader();
			fail("Exception missing...?");
		}
		catch(ProxyException e) {
			asExpected(e);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	protected void getTotal() {
		try {
			config();
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
			int total = pjdcr.getTotal();
			assertTrue(total == 100);
			debug("total: "+total);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

	protected void getMetaCas() {
		try {
			config();
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
			MetaCas mc = pjdcr.getMetaCas();
			int seqNo = mc.getSeqNo();
			asExpected("seqNo = "+seqNo);
			assertTrue(seqNo == 1);
			String serializedCas = (String)mc.getSerializedCas();
			checkCas(serializedCas);
			asExpected("cas = "+serializedCas);
			String documentText = mc.getDocumentText();
			asExpected("documentText = "+documentText);
			assertTrue(documentText.equals("1"));
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

	private void getMetaCases(ProxyJobDriverCollectionReader pjdcr, int total) throws ProxyException {
		for(int c=1; c <= total; c++) {
			MetaCas mc = pjdcr.getMetaCas();
			int seqNo = mc.getSeqNo();
			asExpected("seqNo = "+seqNo);
			assertTrue(seqNo == c);
			String serializedCas = (String)mc.getSerializedCas();
			checkCas(serializedCas);
			asExpected("cas = "+serializedCas);
			String documentText = mc.getDocumentText();
			asExpected("documentText = "+documentText);
			assertTrue(documentText.equals(""+c));
		}
	}
	
	protected void getMetaCases(int extra) {
		try {
			config();
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
			int total = pjdcr.getTotal();
			getMetaCases(pjdcr, total);
			if(extra > 0) {
				for(int j=0; j<extra; j++) {
					MetaCas mc = pjdcr.getMetaCas();
					assertTrue(mc == null);
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}	

}
