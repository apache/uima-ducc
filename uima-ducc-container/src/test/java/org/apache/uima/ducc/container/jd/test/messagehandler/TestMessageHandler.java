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
package org.apache.uima.ducc.container.jd.test.messagehandler;

import static org.junit.Assert.assertTrue;

import org.apache.uima.ducc.container.jd.mh.MessageHandler;
import org.apache.uima.ducc.container.jd.test.TestBase;
import org.apache.uima.ducc.container.net.iface.IMetaCas;
import org.apache.uima.ducc.container.net.iface.IMetaCasTransaction.Type;
import org.apache.uima.ducc.container.net.impl.MetaCasTransaction;
import org.apache.uima.ducc.container.net.impl.TransactionId;

public class TestMessageHandler extends TestBase {
	
	protected boolean enforce = true;
	protected boolean skipAll = true;
	
	private MetaCasTransaction create(String node, int pid, int tid, Type type) {
		MetaCasTransaction mct = new MetaCasTransaction();
		mct.setRequesterNodeName(node);
		mct.setRequesterProcessName(pid+"");
		mct.setRequesterProcessId(pid);
		mct.setRequesterThreadId(tid);
		mct.setType(type);
		return mct;
	}
	
	private IMetaCas transCommon(MessageHandler messageHandler, MetaCasTransaction trans, int reqNo) {
		messageHandler.handleMetaCasTransation(trans);
		IMetaCas metaCas = trans.getMetaCas();
		if(metaCas != null) {
			if(reqNo > 0) {
				String seqNo = ""+reqNo;
				debug("system key:"+metaCas.getSystemKey());
				if(enforce) {
					assertTrue(metaCas.getSystemKey().equals(seqNo));
				}
				asExpected("system key == "+seqNo);
				debug("user key:"+metaCas.getUserKey());
				if(enforce) {
					assertTrue(metaCas.getUserKey().equals(seqNo));
				}
				asExpected("user key == "+seqNo);
			}
		}
		else {
			debug("metaCas is null");
		}
		return metaCas;
	}
	
	protected MetaCasTransaction transGet(MessageHandler messageHandler, String node, int pid, int tid, int reqNo) {
		debug("Get");
		MetaCasTransaction trans = create(node, pid, tid, Type.Get);
		trans.setTransactionId(new TransactionId(reqNo,0));
		transCommon(messageHandler, trans, reqNo);
		return trans;
	}
	
	protected void transAck(MessageHandler messageHandler, MetaCasTransaction trans, int reqNo) {
		debug("Ack");
		trans.setType(Type.Ack);
		trans.setTransactionId(new TransactionId(reqNo,1));
		transCommon(messageHandler, trans, reqNo);
	}
	
	protected void transEnd(MessageHandler messageHandler, MetaCasTransaction trans, int reqNo) {
		debug("End");
		trans.setType(Type.End);
		trans.setTransactionId(new TransactionId(reqNo,2));
		transCommon(messageHandler, trans, reqNo);
	}
	
}
