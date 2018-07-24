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
package org.apache.uima.ducc.ps.service.processor.uima;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler;
import org.apache.uima.ducc.ps.service.errors.builtin.WindowBasedErrorHandler;
import org.apache.uima.ducc.ps.service.utils.UimaSerializer;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class AbstractServiceProcessor {
	// Map to store DuccUimaSerializer instances. Each has affinity to a thread
	protected Map<Long, UimaSerializer> serializerMap = new HashMap<>();
	protected AtomicLong errorCount = new AtomicLong();
	protected AtomicLong successCount = new AtomicLong();
	protected AtomicLong errorCountSinceLastSuccess = new AtomicLong();
	protected int maxErrors=1;  // default is to fail on 1st error
	protected int windowSize=1;
    protected int DEFAULT_INIT_DELAY=30000;
    
	protected IServiceErrorHandler getErrorHandler() {
		// concrete implementation of this abstract class should
		// provide a way to set maxErrors and windowSize. The IServiceProcessor
		// provides a setter method for overriding default values 
		return new WindowBasedErrorHandler().
				withMaxFrameworkErrors(maxErrors).
				withProcessErrorWindow(windowSize).build();

	}

	protected void delay(Logger logger, long howLong) {
		long delay = DEFAULT_INIT_DELAY;
		if ( System.getProperty("ducc.service.init.delay") != null ) {
			delay = Long.parseLong(System.getProperty("ducc.service.init.delay").trim());
		}
		logger.log(Level.INFO, "Wait for the initialized state to propagate to the SM " +
		          "so any processing errors are not treates as initialization failures - delay="+delay);
		try {
			Thread.sleep(delay);
		} catch (InterruptedException e1) {
		}

	}
	protected UimaSerializer getUimaSerializer() {
		
	   	return serializerMap.get(Thread.currentThread().getId());
	}

}
