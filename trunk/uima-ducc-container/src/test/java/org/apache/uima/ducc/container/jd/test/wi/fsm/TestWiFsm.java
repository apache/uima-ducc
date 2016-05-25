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
package org.apache.uima.ducc.container.jd.test.wi.fsm;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.container.common.fsm.Action;
import org.apache.uima.ducc.container.common.fsm.Event;
import org.apache.uima.ducc.container.common.fsm.Fsm;
import org.apache.uima.ducc.container.common.fsm.State;
import org.apache.uima.ducc.container.common.fsm.StateEventKey;
import org.apache.uima.ducc.container.common.fsm.iface.IAction;
import org.apache.uima.ducc.container.common.fsm.iface.IEvent;
import org.apache.uima.ducc.container.common.fsm.iface.IFsm;
import org.apache.uima.ducc.container.common.fsm.iface.IFsmBuilder;
import org.apache.uima.ducc.container.common.fsm.iface.IState;
import org.apache.uima.ducc.container.jd.JobDriver;
import org.apache.uima.ducc.container.jd.JobDriverException;
import org.apache.uima.ducc.container.jd.fsm.wi.WiFsm;
import org.apache.uima.ducc.container.jd.test.TestBase;
import org.apache.uima.ducc.container.jd.test.helper.Utilities;
import org.junit.Before;
import org.junit.Test;

public class TestWiFsm extends TestBase {
	
	protected JobDriver jd;
	
	@Before
    public void setUp() throws JobDriverException {
        initialize();
        jd = JobDriver.getNewInstance();
    }
	
	@Test
	public void test_01() {
		try {
			IState s0 = new State("a");
			IState s1 = new State("a");
			IState s2 = new State("b");
			assertTrue(s0.equals(s1));
			asExpected("State "+s0.getName()+" == "+s1.getName());
			assertTrue(!s0.equals(s2));
			asExpected("State "+s0.getName()+" != "+s2.getName());
			assertTrue(!s1.equals(s2));
			asExpected("State "+s1.getName()+" != "+s2.getName());
			try {
				new State(null);
				fail("expected Exception");
			}
			catch(Exception e) {
				asExpected(e);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_02() {
		try {
			IEvent e0 = new Event("c");
			IEvent e1 = new Event("c");
			IEvent e2 = new Event("d");
			assertTrue(e0.equals(e1));
			asExpected("Event "+e0.getName()+" == "+e1.getName());
			assertTrue(!e0.equals(e2));
			asExpected("Event "+e0.getName()+" != "+e2.getName());
			assertTrue(!e1.equals(e2));
			asExpected("Event "+e1.getName()+" != "+e2.getName());
			try {
				new Event(null);
				fail("expected Exception");
			}
			catch(Exception e) {
				asExpected(e);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_03() {
		try {
			IState s0 = new State("a");
			IState s1 = new State("b");
			IEvent e0 = new Event("c");
			IEvent e1 = new Event("d");
			StateEventKey se00A = new StateEventKey(s0, e0);
			StateEventKey se00B = new StateEventKey(s0, e0);
			assertTrue(se00A.equals(se00B));
			StateEventKey se00 = new StateEventKey(s0, e0);
			StateEventKey se01 = new StateEventKey(s0, e1);
			StateEventKey se10 = new StateEventKey(s1, e0);
			StateEventKey se11 = new StateEventKey(s1, e1);
			assertTrue(!se00.equals(se01));
			assertTrue(!se00.equals(se10));
			assertTrue(!se00.equals(se11));
			assertTrue(!se01.equals(se10));
			assertTrue(!se01.equals(se11));
			assertTrue(!se10.equals(se11));
			try {
				new StateEventKey(null, e0);;
				fail("expected Exception");
			}
			catch(Exception e) {
				asExpected(e);
			}
			try {
				new StateEventKey(s0, null);;
				fail("expected Exception");
			}
			catch(Exception e) {
				asExpected(e);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_04() {
		try {
			IState s1 = new State("s1");
			IState s2 = new State("s2");
			IEvent e1 = new Event("e1");
			IEvent e2 = new Event("e2");
			IAction a0 = new Action();
			IFsmBuilder fsmBuilder = new Fsm();
			IFsm fsm = fsmBuilder;
			debug("s1:"+s1.getName());
			debug("s2:"+s2.getName());
			debug("e1:"+e1.getName());
			debug("e2:"+e2.getName());
			fsmBuilder.addInitial(s1, e1, a0, s1);
			fsmBuilder.add(s1, e2, a0, s2);
			assertTrue(fsm.getStateCurrent().getName().equals(s1.getName()));
			asExpected("state == "+s1.getName());
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	private void initUserClasspath() {
		String userClasspath = Utilities.getInstance().getUserCP();
		String[] classpathParts = userClasspath.split(File.pathSeparator);
		StringBuffer sb = new StringBuffer();
		for(int i=0; i<classpathParts.length; i++) {
			String jar = classpathParts[i];
			debug(i+" use: "+jar);
			sb.append(jar);
			sb.append(File.pathSeparator);
		}
		String userPartialClasspath = sb.toString();
		System.setProperty(FlagsHelper.Name.UserClasspath.pname(), userPartialClasspath);
	}
	
	@Test
	public void test_05() {
		try {
			initUserClasspath();
			WiFsm wiFsm = new WiFsm();
			Object actionData = null;
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
			asExpected("state == "+WiFsm.Start.getName());
			wiFsm.transition(WiFsm.Get_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Get_Pending.getName()));
			asExpected("state == "+WiFsm.Get_Pending.getName());
			wiFsm.transition(WiFsm.CAS_Available, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.CAS_Send.getName()));
			asExpected("state == "+WiFsm.CAS_Send.getName());
			wiFsm.transition(WiFsm.Ack_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.CAS_Active.getName()));
			asExpected("state == "+WiFsm.CAS_Active.getName());
			wiFsm.transition(WiFsm.End_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
			asExpected("state == "+WiFsm.Start.getName());
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
	
	@Test
	public void test_06() {
		try {
			initUserClasspath();
			WiFsm wiFsm = new WiFsm();
			Object actionData = null;
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
			asExpected("state == "+WiFsm.Start.getName());
			wiFsm.transition(WiFsm.Process_Preempt, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
			//
			wiFsm.transition(WiFsm.Get_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Get_Pending.getName()));
			wiFsm.transition(WiFsm.Process_Preempt, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
			//
			wiFsm.transition(WiFsm.Get_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Get_Pending.getName()));
			asExpected("state == "+WiFsm.Get_Pending.getName());
			wiFsm.transition(WiFsm.CAS_Available, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.CAS_Send.getName()));
			asExpected("state == "+WiFsm.CAS_Send.getName());
			wiFsm.transition(WiFsm.Process_Preempt, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
			//
			wiFsm.transition(WiFsm.Get_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Get_Pending.getName()));
			asExpected("state == "+WiFsm.Get_Pending.getName());
			wiFsm.transition(WiFsm.CAS_Available, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.CAS_Send.getName()));
			asExpected("state == "+WiFsm.CAS_Send.getName());
			wiFsm.transition(WiFsm.Ack_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.CAS_Active.getName()));
			asExpected("state == "+WiFsm.CAS_Active.getName());
			wiFsm.transition(WiFsm.Process_Preempt, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
			//
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
			asExpected("state == "+WiFsm.Start.getName());
			wiFsm.transition(WiFsm.Get_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Get_Pending.getName()));
			asExpected("state == "+WiFsm.Get_Pending.getName());
			wiFsm.transition(WiFsm.CAS_Available, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.CAS_Send.getName()));
			asExpected("state == "+WiFsm.CAS_Send.getName());
			wiFsm.transition(WiFsm.Ack_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.CAS_Active.getName()));
			asExpected("state == "+WiFsm.CAS_Active.getName());
			wiFsm.transition(WiFsm.End_Request, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
			asExpected("state == "+WiFsm.Start.getName());
			wiFsm.transition(WiFsm.Process_Preempt, actionData);
			assertTrue(wiFsm.getStateCurrent().getName().equals(WiFsm.Start.getName()));
		}
		catch(Exception e) {
			e.printStackTrace();
			fail("Exception");
		}
	}
}
