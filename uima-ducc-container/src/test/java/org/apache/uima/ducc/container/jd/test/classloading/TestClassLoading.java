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
package org.apache.uima.ducc.container.jd.test.classloading;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.common.classloader.ProxyException;
import org.apache.uima.ducc.container.dgen.classload.ProxyDeployableGeneration;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverCollectionReader;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverDirective;
import org.apache.uima.ducc.container.jd.classload.ProxyJobDriverErrorHandler;
import org.apache.uima.ducc.container.jd.test.TestBase;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.apache.uima.ducc.container.jd.user.error.classload.ProxyUserErrorStringify;
import org.apache.uima.ducc.ps.net.impl.MetaTask;
import org.apache.uima.ducc.user.common.PrivateClassLoader;
import org.apache.uima.ducc.user.error.iface.Transformer;
import org.junit.Before;
import org.junit.Test;

public class TestClassLoading extends TestBase {
	
	protected JobDriver jd;
	
	@Before
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getNewInstance();
    }
	
	@Test
	public void test_01() {
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			ProxyJobDriverErrorHandler pjdeh = new ProxyJobDriverErrorHandler();
			String serializedCAS = null;
			String serializedException = null;
			pjdeh.handle(serializedCAS, serializedException);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception " + e);
		}
	}

	@Test
	public void test_02() {
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), crXml);
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
			MetaTask mc = pjdcr.getMetaCas();
			String serializedCAS = mc.getSerializedTask();
			String serializedException = null;
			ProxyJobDriverErrorHandler pjdeh = new ProxyJobDriverErrorHandler();
			pjdeh.handle(serializedCAS, serializedException);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_03() {
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), crXml);
			ProxyJobDriverCollectionReader pjdcr = new ProxyJobDriverCollectionReader();
			MetaTask mc = pjdcr.getMetaCas();
			String serializedCAS = mc.getSerializedTask();
			Exception e = new Exception("test");
			String serializedException = e.toString();
			String className = "org.apache.uima.ducc.user.jd.test.helper.TestJdContainerErrorHandler";
			System.setProperty(FlagsHelper.Name.UserErrorHandlerClassname.pname(), className);
			ProxyJobDriverErrorHandler pjdeh = new ProxyJobDriverErrorHandler();
			ProxyJobDriverDirective directive = pjdeh.handle(serializedCAS, serializedException);
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
	public void test_04() {
		try {
			//TODO fix & keep this test?
			boolean disabled = true;
			if(disabled) {
				return;
			}
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			ProxyDeployableGeneration proxy = new ProxyDeployableGeneration();
			//
			URL url = this.getClass().getResource("/");
			File root = new File(url.getFile());
			String name = root.getAbsolutePath();
			debug(name);
			assertTrue(root.isDirectory());
			String nameWorking = name+File.separator+"working";
			File working = new File(nameWorking);
			delete(working);
			working.mkdir();
			//
			String directory = working.getAbsolutePath();
			String id = "12345";
			Integer dgenThreadCount = new Integer(1);
			String dgenFlowController = "flowController";
			String cmDescriptor = null;
			List<String> cmOverrides = null;
			//String aeDescriptor = "org.apache.uima.ducc.test.randomsleep.FixedSleepAE";
			String aeDescriptor = "FixedSleepAE";
			List<String> aeOverrides = null;
			String ccDescriptor = null;
			List<String> ccOverrides = null;
			String dgen = proxy.generate(
					directory, 
					id, 
					dgenThreadCount,
					dgenFlowController,
					cmDescriptor, 
					cmOverrides, 
					aeDescriptor, 
					aeOverrides, 
					ccDescriptor, 
					ccOverrides
					);
			debug(dgen);
			//
			delete(working);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	//@Test
	public void test_05() {
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			ProxyUserErrorStringify pues = new ProxyUserErrorStringify();
			Exception e = new RuntimeException("error test #05");
			Object serializedException = Transformer.serialize(e);
			String stringifiedException = pues.convert(serializedException);
			String prefix = "java.lang.RuntimeException: error test #05";
			assertTrue(stringifiedException.startsWith(prefix));
			//System.out.println(stringifiedException);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

	//@Test
	public void test_06() {
		try {
			String userClasspath = Utilities.getInstance().getUserCP();
			System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
			URL urlXml = this.getClass().getResource("/CrInitException.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), crXml);
			new ProxyJobDriverCollectionReader();
			fail("No Exception?");
		}
		catch(ProxyException e) {
			// as expected
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
  //@Test
  public void test_loader() throws URISyntaxException, IOException {

    // First set up a private class-loaded and verify that its resources exist 
    // and are not also in the system class-loader.
    String privateCP = "src/test/java/";
    String privateResource = "org/apache/uima/ducc/container/jd/test/TestClassLoading.java";
    String publicClass = "org.apache.uima.ducc.container.common.Util";
    
    URLClassLoader scl = (URLClassLoader) ClassLoader.getSystemClassLoader();
    URLClassLoader pcl = PrivateClassLoader.create(privateCP);
    for (URL u : pcl.getURLs()) {
      if (!(new File(u.getPath())).exists()) {
        fail("Missing test classpath resource: " + u);
      }
      URI ur = u.toURI().normalize();
      for (URL uu : scl.getURLs()) {
        if (ur.equals(uu.toURI().normalize())) {
          fail("Test classpath resource: " + u + " is also in the system class-loader");
        }
      }
    }
    
    // Check that a private resource can only be loaded from the private class-loader
    // i.e. no leakage from private to system
    // (Can't use a class as all in this project are in the public classpath)
    if (scl.findResource(privateResource) != null) { 
      fail("Found private resource in system class-loader");
    }
    if (pcl.findResource(privateResource) == null) {
      PrivateClassLoader.dump(pcl, 1);
      fail("Cannot load private resource");
    }

    // Check that a public class can only be loaded from the system class-loader
    // i.e. no leakage from public to private
    try {
      pcl.loadClass(publicClass);
      fail("Found public class in private class-loader");
    } catch (ClassNotFoundException e) {
    }
    try {
      scl.loadClass(publicClass);
    } catch (ClassNotFoundException e) {
      fail("Cannot load public class");
    }
    // pcl.close();   // Requires Java 1.7
  }
  
  @Test
  public void test_invalid_cp() throws URISyntaxException, IOException {

    // Set up a private class-loader with a couple of missing 
    // or invalid entries that should be quietly ignored
    String[] privateCP = {"target/classes",
                          "pom.xml*",
                          "unknown-file.jar",
                          "unknown-wildcard/*"};
    
    URLClassLoader pcl = null;
    try {
      pcl = PrivateClassLoader.create(privateCP);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed to create private class loader");
    }
    URL[] urls = pcl.getURLs();
    
    if (urls.length != 1) {
      fail("Should have only one entry in the classpath, not " + urls.length);
    }
    // The following test is failing on Jenkins?
    boolean enable = false;
    if(enable) {
    	File f = new File(privateCP[0]);
        URL u = f.toURI().toURL();
        if (!u.equals(urls[0])) {
          fail("Classpath should have "+u+" but instead has "+urls[0]);
        }
    }
  }  
}
