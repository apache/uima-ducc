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
package org.apache.uima.ducc.container.common.logger;

import org.apache.uima.ducc.container.common.logger.id.Id;

public interface ILogger {

	public Id null_id = Id.null_id;
	
    public void fatal(String location, Id jobId, Object ... args);
    public void fatal(String location, Id jobId, Throwable t, Object ... args);
    public void fatal(String location, Id jobId, Id processId, Object ... args);
    public void fatal(String location, Id jobId, Id processId, Throwable t, Object ... args);
    
    public void debug(String location, Id jobId, Object ... args);
    public void debug(String location, Id jobId, Throwable t, Object ... args);
    public void debug(String location, Id jobId, Id processId, Object ... args);
    public void debug(String location, Id jobId, Id processId, Throwable t, Object ... args);
    
    public void error(String location, Id jobId, Object ... args);
    public void error(String location, Id jobId, Throwable t, Object ... args);    
    public void error(String location, Id jobId, Id processId, Object ... args);
    public void error(String location, Id jobId, Id processId, Throwable t, Object ... args);
    
    public void info(String location, Id jobId, Object ... args);
    public void info(String location, Id jobId, Throwable t, Object ... args);    
    public void info(String location, Id jobId, Id processId, Object ... args);
    public void info(String location, Id jobId, Id processId, Throwable t, Object ... args);
    
    public void trace(String location, Id jobId, Object ... args);
    public void trace(String location, Id jobId, Throwable t, Object ... args);    
    public void trace(String location, Id jobId, Id processId, Object ... args);
    public void trace(String location, Id jobId, Id processId, Throwable t, Object ... args);
    
    public void warn(String location, Id jobId, Object ... args);
    public void warn(String location, Id jobId, Throwable t, Object ... args);
    public void warn(String location, Id jobId, Id processId, Object ... args);
    public void warn(String location, Id jobId, Id processId, Throwable t, Object ... args);

}
