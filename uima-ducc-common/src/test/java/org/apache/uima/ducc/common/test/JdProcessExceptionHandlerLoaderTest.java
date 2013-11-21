package org.apache.uima.ducc.common.test;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.common.jd.plugin.IJdProcessExceptionHandler;
import org.apache.uima.ducc.common.jd.plugin.JdProcessExceptionHandler;
import org.apache.uima.ducc.common.jd.plugin.JdProcessExceptionHandlerLoader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdProcessExceptionHandlerLoaderTest {

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

	@Test
	public void test() {
		try {
			IJdProcessExceptionHandler jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load(JdProcessExceptionHandler.class.getName());
			CAS cas = null;
			Exception e = null;
			Properties p = null;
			jdProcessExceptionHandler.handle("test001",cas, e, p);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (InstantiationException e) {
			e.printStackTrace();
			fail("Exception");
		}
			
		try {
			IJdProcessExceptionHandler jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load("org.apache.uima.ducc.common.jd.plugin.example.ExampleJdProcessExceptionHandler");
			CAS cas = null;
			Exception e = null;
			Properties p = null;
			jdProcessExceptionHandler.handle("test002",cas, e, p);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (InstantiationException e) {
			e.printStackTrace();
			fail("Exception");
		}
		
		try {
			IJdProcessExceptionHandler jdProcessExceptionHandler = JdProcessExceptionHandlerLoader.load("org.apache.uima.ducc.common.jd.plugin.example.BadJdProcessExceptionHandler");
			CAS cas = null;
			Exception e = null;
			Properties p = null;
			jdProcessExceptionHandler.handle("test003",cas, e, p);
			fail("No Exception?");
		} catch (ClassNotFoundException e) {
			//Expected
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			fail("Exception");
		} catch (InstantiationException e) {
			e.printStackTrace();
			fail("Exception");
		}
	}

}
