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
package org.apache.uima.ducc.jd.client;

import java.lang.Thread.UncaughtExceptionHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.uima.ducc.common.internationalization.Messages;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.jd.JobDriverContext;


/**
 * Dynamic Thread Pool
 */

public class DynamicThreadPoolExecutor extends ThreadPoolExecutor {
	
	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(DynamicThreadPoolExecutor.class.getName());
	private static Messages duccMsg = JobDriverContext.getInstance().getSystemMessages();
	
	private DuccId duccId;
	private Semaphore terminations = new Semaphore(0);
	
	private String keyUimaAsClientTracking = "UimaAsClientTracking";
	private boolean uimaAsClientTracking = false;
	
	public DynamicThreadPoolExecutor(int corePoolSize, 
			int maximumPoolSize,
			long keepAliveTime, 
			TimeUnit unit,
			BlockingQueue<Runnable> workQueue, 
			ThreadFactory threadFactory,
			DuccId duccId) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
				workQueue, threadFactory);
		String methodName = "DynamicThreadPoolExecutor";
		this.duccId = duccId;
		duccOut.trace(methodName, duccId, duccMsg.fetch("enter"));
		if(System.getProperty(keyUimaAsClientTracking) != null) {
			uimaAsClientTracking = true;
		}
		duccOut.trace(methodName, duccId, duccMsg.fetch("exit"));
	}
    /**
     * This is an overidden method that will be called before a job is run.
     * The intent here is to stop a thread to force its removal from a thread
     * pool. This is done by throwing a StopThreadException. This is the only
     * way to force thread removal from a thread pool. When a thread pool
     * size is redefined (by calling setCorePoolSize() below, a semaphore 
     * is given a number of permits. This number of permits is equal to a number
     * of threads we need to stop. This method first attempts to acquire a
     * permit and then throws an exception signaling the ThreadPoolExecutor to
     * kill a given thread. 
     */
	protected void beforeExecute(final Thread thread, final WorkItem workItem) {
		String methodName = "beforeExecute";
		duccOut.trace(methodName, duccId, duccMsg.fetch("enter"));
		if (terminations.tryAcquire()) {
			String text = duccMsg.fetch("kill thread for")+" "+duccMsg.fetch("seqNo")+" "+workItem.getSeqNo();
			duccOut.debug(methodName, duccId, text);
			// Since a given thread will be soon killed we need to re-queue
			// the work so it is not lost. The work is appended to the queue.
			// Perhaps it should be placed at the head, to keep the correct
			// sequence. For DUCC this may not be an issue.
			super.getQueue().add(workItem);
			thread.setUncaughtExceptionHandler(new ShutdownHandler(duccId, thread.getUncaughtExceptionHandler()));
			// Throw an exception to signal ThreadPoolExecutor to remove the thread
			// from its pool.
			throw new StopThreadException(duccId, duccMsg.fetch("terminating thread"));
		}
		else {
			String text = duccMsg.fetch("no")+" "+duccMsg.fetch("kill thread for")+" "+duccMsg.fetch("seqNo")+" "+workItem.getSeqNo();
			duccOut.debug(methodName, duccId, text);
		}
		duccOut.trace(methodName, duccId, duccMsg.fetch("exit"));
	}
	/**
	 * This method should be called to change the number of processing threads. This
	 * should be done when the Scheduler sends new job node assignments.
	 */
	public void setCorePoolSize(final int size) {
		String methodName = "setCorePoolSize";
		duccOut.trace(methodName, duccId, duccMsg.fetch("enter"));
		if(size > 0) {
			int delta = getMaximumPoolSize() - size;
			if(delta != 0) {
				String message = duccMsg.fetch("changing core pool size to")+" "+size+" "+duccMsg.fetch("from")+" "+getMaximumPoolSize();
				if(uimaAsClientTracking) {
					duccOut.info(methodName, duccId, message);
				}
				super.setCorePoolSize(size);
				super.setMaximumPoolSize(size);
			}
			if (delta > 0) {
				String text = duccMsg.fetch("releasing")+" "+delta+" "+duccMsg.fetch("permits to force thread terminations");
				duccOut.debug(methodName, duccId, text);
				terminations.release(delta);
			}
		}
		else {
			String text = duccMsg.fetch("size zero request ignored");
			duccOut.debug(methodName, duccId, text);
		}
		duccOut.trace(methodName, duccId, duccMsg.fetch("exit"));
	}
	
	public int changeCorePoolSize(final int size) {
		int old_size = getMaximumPoolSize();
		setCorePoolSize(size);
		return size - old_size;
	}
	
	@SuppressWarnings("serial")
	private class StopThreadException extends RuntimeException {
		StopThreadException(DuccId duccId, String message) {
			super(message);
			String methodName = "StopThreadException";
			duccOut.trace(methodName, duccId, duccMsg.fetch("enter"));
			duccOut.trace(methodName, duccId, duccMsg.fetch("exit"));
		}
	}

	/**
	 * This uncaught exception handler is used only as threads are entered into
	 * their shutdown state.
	 */
	private class ShutdownHandler implements UncaughtExceptionHandler {
		
		private DuccId duccId;
		private UncaughtExceptionHandler handler;

		/**
		 * Create a new shutdown handler.
		 * 
		 * @param handler
		 *            The original handler to delegate non-shutdown exceptions to.
		 */
		ShutdownHandler(DuccId duccId, UncaughtExceptionHandler handler) {
			String methodName = "ShutdownHandler";
			duccOut.trace(methodName, duccId, duccMsg.fetch("enter"));
			this.duccId = duccId;
			this.handler = handler;
			duccOut.trace(methodName, duccId, duccMsg.fetch("exit"));
		}

		/**
		 * Quietly ignore {@link StopThreadException}.
		 * <p>
		 * Do nothing if this is a StopThreadException, this is just to prevent
		 * logging an uncaught exception which is expected. Otherwise forward it
		 * to the thread group handler (which may hand it off to the default
		 * uncaught exception handler).
		 * </p>
		 */
		public void uncaughtException(Thread thread, Throwable throwable) {
			String methodName = "uncaughtException";
			duccOut.trace(methodName, duccId, duccMsg.fetch("enter"));
			if (!(throwable instanceof StopThreadException)) {
				/*
				 * Use the original exception handler if one is available,
				 * otherwise use the group exception handler.
				 */
				if (handler != null) {
					handler.uncaughtException(thread, throwable);
				}
			}
			duccOut.trace(methodName, duccId, duccMsg.fetch("exit"));
		}
	}

}

