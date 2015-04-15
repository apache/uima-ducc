package org.apache.uima.ducc.container.jd.test;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;

public class TestBase {
	
	private boolean debug = false;

	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
	
	public void initialize() throws JobDriverException {
		if(!debug) {
			System.setOut(new PrintStream(outContent));
			System.setErr(new PrintStream(errContent));
		}
		ducc_home();
		jd_setup();
	}
	
	public String getResource(String name) {
		String retVal = null;
		URL urlXml = null;
		File file = null;
		String path = null;
		//
		urlXml = this.getClass().getResource(name);
		file = new File(urlXml.getFile());
		path = file.getAbsolutePath();
		retVal = path;
		return retVal;
	}
	
	public void ducc_home() {
		String folder = "/ducc_runtime";
		String file = "/resources/log4j.xml";
		String path = getResource(folder+file);
		String value = path.replace(file, "");
		String key = "DUCC_HOME";
		System.setProperty(key, value);
	}
	
	public void jd_setup() throws JobDriverException {
		File working = mkWorkingDir();
		String directory = working.getAbsolutePath();
		System.setProperty(FlagsHelper.Name.JobDirectory.pname(), directory);
		//
		URL urlXml = null;
		File file = null;
		String path = null;
		//
		urlXml = this.getClass().getResource("/CR100.xml");
		file = new File(urlXml.getFile());
		path = file.getAbsolutePath();
		System.setProperty(FlagsHelper.Name.CollectionReaderXml.pname(), path);
		//
		urlXml = this.getClass().getResource("/DDSleepDescriptor.xml");
		file = new File(urlXml.getFile());
		path = file.getAbsolutePath();
		System.setProperty(FlagsHelper.Name.JpDd.pname(), path);
		//
		String userClasspath = Utilities.getInstance().getUserCP();
		System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userClasspath);
	}
	
	public boolean isDebug() {
		return debug;
	}
	
	protected void out_println(String message) {
		System.out.println(message);
	}
	
	protected void debug(String message) {
		if(isDebug()) {
			out_println(message);
		}
	}
	
	protected void asExpected(String text) {
		if(isDebug()) {
			String message = "as expected: "+text;
			out_println(message);
		}
	}
	
	protected void asExpected(Exception e) {
		if(isDebug()) {
			String message = "as expected: "+e.getMessage();
			out_println(message);
		}
	}
	
	protected void delete(File directory) {
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
	
	protected File mkWorkingDir() {
		URL url = this.getClass().getResource("/");
		File root = new File(url.getFile());
		String name = root.getAbsolutePath();
		debug(name);
		assertTrue(root.isDirectory());
		String nameWorking = name+File.separator+"working";
		File working = new File(nameWorking);
		delete(working);
		working.mkdir();
		return working;
	}
}
