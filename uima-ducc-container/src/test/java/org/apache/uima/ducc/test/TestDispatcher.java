package org.apache.uima.ducc.test;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URL;

import org.apache.uima.ducc.container.jd.JobDriverCommon;
import org.apache.uima.ducc.container.jd.dispatch.Dispatcher;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.container.net.impl.TransactionId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDispatcher {

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
	private boolean asExpected = true;
	
	private void out_println(String message) {
		System.out.println(message);
	}
	
	private void debug(String message) {
		if(debug) {
			out_println(message);
		}
	}
	
	private void asExpected(String text) {
		if(asExpected) {
			String message = "as expected: "+text;
			out_println(message);
		}
	}
	
	/*
	private void asExpected(Exception e) {
		if(asExpected) {
			String message = "as expected: "+e.getMessage();
			out_println(message);
		}
	}
	*/
	
	private MetaCasTransaction create(String node, int pid, int tid, Type type) {
		MetaCasTransaction mct = new MetaCasTransaction();
		mct.setRequesterName(node);
		mct.setRequesterProcessId(pid);
		mct.setRequesterThreadId(tid);
		mct.setType(type);
		return mct;
	}
	
	private IMetaCas transCommon(Dispatcher dispatcher, MetaCasTransaction trans, int reqNo) {
		dispatcher.handleMetaCasTransation(trans);
		IMetaCas metaCas = trans.getMetaCas();
		if(metaCas != null) {
			String seqNo = ""+reqNo;
			debug("system key:"+metaCas.getSystemKey());
			assertTrue(metaCas.getSystemKey().equals(seqNo));
			asExpected("system key == "+seqNo);
			debug("user key:"+metaCas.getUserKey());
			assertTrue(metaCas.getUserKey().equals(seqNo));
			asExpected("user key == "+seqNo);
		}
		else {
			debug("metaCas is null");
		}
		return metaCas;
	}
	
	private IMetaCas transGet(Dispatcher dispatcher, String node, int pid, int tid, int reqNo) {
		debug("Get");
		MetaCasTransaction trans = create(node, pid, tid, Type.Get);
		trans.setTransactionId(new TransactionId(reqNo,0));
		return transCommon(dispatcher, trans, reqNo);
	}
	
	private IMetaCas transAck(Dispatcher dispatcher, String node, int pid, int tid, int reqNo) {
		debug("Ack");
		MetaCasTransaction trans = create(node, pid, tid, Type.Ack);
		trans.setTransactionId(new TransactionId(reqNo,1));
		return transCommon(dispatcher, trans, reqNo);
	}
	
	private IMetaCas transEnd(Dispatcher dispatcher, String node, int pid, int tid, int reqNo) {
		debug("End");
		MetaCasTransaction trans = create(node, pid, tid, Type.End);
		trans.setTransactionId(new TransactionId(reqNo,2));
		return transCommon(dispatcher, trans, reqNo);
	}
	
	@Test
	public void test_01() {
		try {
			String[] jarList260 = { 
					"/ducc-user.jar",
					"/uimaj-as-core-2.6.0.jar",
					"/uimaj-core-2.6.0.jar",
					"/xstream-1.3.1.jar"
			};
			URL urlXml = this.getClass().getResource("/CR100.xml");
			File file = new File(urlXml.getFile());
			String crXml = file.getAbsolutePath();
			String crCfg = null;
			new JobDriverCommon(jarList260, crXml, crCfg);
			int size = JobDriverCommon.getInstance().getMap().size();
			debug("map size:"+size);
			Dispatcher dispatcher = new Dispatcher();
			String node = "node01";
			int pid = 23;
			int tid = 45;
			int casNo = 1;
			IMetaCas metaCasPrevious = null;
			IMetaCas metaCas = transGet(dispatcher,node,pid,tid,casNo);
			while(metaCas != null) {
				transAck(dispatcher,node,pid,tid,casNo);
				transEnd(dispatcher,node,pid,tid,casNo);
				casNo++;
				metaCasPrevious = metaCas;
				metaCas = transGet(dispatcher,node,pid,tid,casNo);
			}
			assertTrue(metaCasPrevious.getSystemKey().equals("100"));
			asExpected("CASes processed count == 100");
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
}
