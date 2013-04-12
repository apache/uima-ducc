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
package org.apache.uima.ducc.cli.aio;

public interface IMessageHandler {

	public enum Level 
				{ 
					UserInfo 
					{	public String getLabel() { return "uI"; } 	
					}, 
					UserError 
					{	public String getLabel() { return "uE"; }	
					}, 	
					UserWarn 
					{	public String getLabel() { return "uW"; }	
					}, 	
					UserDebug 
					{	public String getLabel() { return "uD"; }	
					}, 
					UserTrace 
					{	public String getLabel() { return "uT"; }	
					}, 
					FrameworkInfo 
					{	public String getLabel() { return "fI"; }	
					}, 
					FrameworkError 
					{	public String getLabel() { return "fE"; }	
					}, 
					FrameworkWarn 
					{	public String getLabel() { return "fW"; }	
					}, 
					FrameworkDebug 
					{	public String getLabel() { return "fD"; }	
					}, 
					FrameworkTrace 
					{	public String getLabel() { return "fT"; }	
					},
					;
					public abstract String  getLabel();
				};
	
	
	
	public enum Toggle { On, Off };
	
	public void setLevel(Level level, Toggle toggle);
	public Toggle getLevel(Level level);
	
	public void setTimestamping(Toggle toggle);
	public Toggle getTimestamping();
	
	public void info(String message);
	public void error(String message);
	public void error(Exception e);
	public void error(Throwable t);
	public void warn(String message);
	public void debug(String message);
	public void trace(String message);
	
	public void info(String cid, String mid, String message);
	public void error(String cid, String mid, String message);
	public void warn(String cid, String mid, String message);
	public void debug(String cid, String mid, String message);
	public void trace(String cid, String mid, String message);
	
	public void frameworkInfo(String message);
	public void frameworkError(String message);
	public void frameworkError(Exception e);
	public void frameworkError(Throwable t);
	public void frameworkWarn(String message);
	public void frameworkDebug(String message);
	public void frameworkTrace(String message);

	public void frameworkInfo(String cid, String mid, String message);
	public void frameworkWarn(String cid, String mid, String message);
	public void frameworkError(String cid, String mid, String message);
	public void frameworkDebug(String cid, String mid, String message);
	public void frameworkTrace(String cid, String mid, String message);
	
}
