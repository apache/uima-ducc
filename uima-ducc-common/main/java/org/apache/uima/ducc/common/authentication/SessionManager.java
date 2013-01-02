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
package org.apache.uima.ducc.common.authentication;

import java.security.SecureRandom;
import java.util.Calendar;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Server side session management.
 * 
 *  A session is established for authenticated users. A session id is created, using 
 *  a secure random number generator, and is associated with the user id. Also the
 *  TOD at session establishment is associated with the user id. All three values
 *  values are kept in a server side volatile cache, lost when the server recycles.
 *  
 *  Each time the user requests an operation that requires authentication, the session
 *  cache is checked. If a non-expired session is already established, then the 
 *  operation is allowed, else the user is requested to re-authenticate. 
 *  
 *  The default session expiry is 30 minutes.
 *  
 *  If an invalid session id is ever presented for the user id, immediately invalidate
 *  the associated session to force re-login.
 */
public class SessionManager {
	
	private static Logger logger = Logger.getLogger(SessionManager.class);

	public static final int millis_per_second = 1000;
	public static final int seconds_per_minute = 60;
	
	public static final int DefaultSessionLifetimeMillis = 30*seconds_per_minute*millis_per_second; // 30 minutes in milliseconds
	
	private static SecureRandom sr = new SecureRandom();
	
	private ConcurrentHashMap<String,ConcurrentHashMap<String,String>> userMap = new ConcurrentHashMap<String,ConcurrentHashMap<String,String>>();
	
	private int sessionLifetimeMillis = DefaultSessionLifetimeMillis;
	
	private static SessionManager instance = new SessionManager();
	
	public static SessionManager getInstance() 
	{
		return instance;
	}
	
	public SessionManager()
	{
	}
	
	public SessionManager(int sessionLifetimeMillis)
	{
		setSessionLifetimeMillis(sessionLifetimeMillis);
	}
	
	public int getSessionLifetimeMillis()
	{
		return sessionLifetimeMillis;
	}
	
	public void setSessionLifetimeMillis(int sessionLifetimeMillis)
	{
		this.sessionLifetimeMillis = sessionLifetimeMillis;
	}
	
	public String register(String userId)
	{
		logger.trace("register.enter");
		String sessionId = null;
		String timestamp = null;
		if (userId != null) {
			if(!userMap.containsKey(userId)) {
				ConcurrentHashMap<String,String> sessionMap = new ConcurrentHashMap<String,String>();
				userMap.putIfAbsent(userId, sessionMap);
			}
			ConcurrentHashMap<String,String> sessionMap = userMap.get(userId);
			sessionId = ""+sr.nextLong();
			timestamp = ""+Calendar.getInstance().getTimeInMillis();
			sessionMap.put(sessionId, timestamp);
			logger.info("register"+_userId_+userId+_sessionId_+sessionId+_timestamp_+timestamp);
		}
		else {
			logger.error("register"+_userId_+userId+_sessionId_+sessionId+_timestamp_+timestamp);
		}
		logger.trace("register.exit");
		return sessionId;
	}
	
	public String unRegister(String userId, String sessionId)
	{
		logger.trace("unRegister.enter");
		if ((userId != null) && (sessionId != null)) {
			if(userMap.containsKey(userId)) {
				ConcurrentHashMap<String,String> sessionMap = userMap.get(userId);
				if(sessionMap.containsKey(sessionId)) {
					String timestamp = sessionMap.get(sessionId);
					sessionMap.remove(sessionId);
					logger.info("unregister"+_userId_+userId+_sessionId_+sessionId+_timestamp_+timestamp);
				}
				else {
					logger.debug("unRegister"+_userId_+userId+_sessionId_+sessionId+" not found");
				}
			}
			else {
				logger.debug("unRegister"+_userId_+userId+" not found");
			}
		}
		else {
			logger.error("unregister"+_userId_+userId+_sessionId_+sessionId);
		}
		logger.trace("unRegister.exit");
		return sessionId;
	}
	
	public class SecurityException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	public boolean validateRegistration(String userId, String sessionId) throws SecurityException
	{
		logger.trace("validateRegistration.enter");
		boolean retVal = false;
		if ((userId != null) && (sessionId != null)) {
			if(userMap.containsKey(userId)) {
				ConcurrentHashMap<String,String> sessionMap = userMap.get(userId);
				if(sessionMap.containsKey(sessionId)) {
					String timestamp = sessionMap.get(sessionId);
					if(isRegistrationExpired(timestamp)) {
						logger.debug("validateRegistration"+_userId_+userId+_sessionId_+sessionId+" expired");
					}
					else {
						logger.debug("validateRegistration"+_userId_+userId+_sessionId_+sessionId+" valid");
						retVal = true;
					}
				}
				else {
					logger.debug("validateRegistration"+_userId_+userId+_sessionId_+sessionId+" not found");
				}
			}
			else {
				logger.debug("validateRegistration"+_userId_+userId+" not found");
			}
		}
		else {
			logger.debug("validateRegistration"+_userId_+userId+_sessionId_+sessionId);
		}
		logger.trace("validateRegistration.exit");
		return retVal;
	}
	
	private boolean isRegistrationExpired(String timestamp)
	{
		logger.trace("isRegistrationExpired.enter");
		boolean retVal = true;
		if(timestamp != null) {
			try {
				long tod = Calendar.getInstance().getTimeInMillis();
				long sessionStart =Long.parseLong(timestamp);
				long expiry = sessionStart+sessionLifetimeMillis;
				logger.debug("isRegistrationExpired"+_timestamp_+timestamp+_tod_+tod);
				if (tod <= expiry) {
					retVal = false;
				}
			}
			catch(Throwable  t) {
				logger.debug(t);
			}
		}
		else {
			logger.debug("isRegistrationExpired"+_timestamp_+timestamp);
		}
		logger.trace("isRegistrationExpired.exit");
		return retVal;
	}
	
	//private static String  userId_    =  "userId: ";
	private static String _userId_    = " userId: ";
	private static String _sessionId_ = " sessionId: ";
	private static String _timestamp_ = " timestamp: ";
	//private static String _expiry_    = " expiry: ";
	private static String _tod_       = " tod: ";
	
	/*
	 * <test cases>
	 */
	
	private static void test01(Logger logt, SessionManager sessionManager) 
	{
		logt.trace("test01.enter");
		String userid = null;
		String sessionId = null;
		sessionId = sessionManager.register(userid);
		assert(sessionId == null);
		userid = "user01";
		sessionId = sessionManager.register(userid);
		assert(sessionId != null);
		logt.trace("test01.exit");
	}
	
	private static void test02(Logger logt, SessionManager sessionManager) 
	{
		logt.trace("test02.enter");
		String userid = null;
		String sessionId = null;
		sessionId = sessionManager.unRegister(userid,sessionId);
		assert(sessionId == null);
		userid = "user02";
		sessionId = sessionManager.register(userid);
		assert(sessionId != null);
		assert(sessionId.equals(sessionManager.unRegister(userid,sessionId)));
		userid = "user00";
		sessionId = sessionManager.unRegister(userid,sessionId);
		assert(sessionId == null);
		logt.trace("test02.exit");
	}
	
	private static void test03(Logger logt, SessionManager sessionManager) throws SecurityException 
	{
		logt.trace("test03.enter");
		String userid = null;
		String sessionId = null;
		assert(!sessionManager.validateRegistration(null,null));
		userid = "user03";
		assert(!sessionManager.validateRegistration(userid,null));
		sessionId = sessionManager.register(userid);
		assert(!sessionManager.validateRegistration(userid,null));
		assert(sessionManager.validateRegistration(userid,sessionId));
		assert(sessionId.equals(sessionManager.unRegister(userid,sessionId)));
		assert(!sessionManager.validateRegistration(userid,sessionId));
		userid = "user03a";
		sessionId = sessionManager.register(userid);
		assert(sessionId != null);
		assert(sessionManager.validateRegistration(userid,sessionId));
		try {
			sessionManager.validateRegistration(userid,"foobar");
			assert(false);
		}
		catch(SecurityException e) {
			logt.debug("recevied expected SecurityException");
		}
		assert(!sessionManager.validateRegistration(userid,sessionId));
		logt.trace("test03.exit");
	}
	
	private static void test04(Logger logt, SessionManager sessionManager, int sessionLifetime) throws SecurityException 
	{
		logt.trace("test04.enter");
		String userid = "user04";
		String sessionId = null;
		sessionId = sessionManager.register(userid);
		assert(sessionManager.validateRegistration(userid,sessionId));
		try {
			Thread.sleep(sessionLifetime);
		} catch (InterruptedException e) {
		}
		assert(!sessionManager.validateRegistration(userid,sessionId));
		logt.trace("test04.exit");
	}
	
	private static void test(String[] args) throws SecurityException {
		Logger logt = Logger.getLogger("Test");
		logt.trace("test.enter");
		int testSessionLifetime = 2*1000; // 2 seconds in milliseconds
		SessionManager sessionManager = new SessionManager(testSessionLifetime);
		test01(logt,sessionManager);
		test02(logt,sessionManager);
		test03(logt,sessionManager);
		test04(logt,sessionManager,testSessionLifetime);
		logt.trace("test.exit");
	}
	
	public static void main(String[] args) {
		try{
			test(args);
			System.out.println("Successs!");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * </test cases>
	 */
}
