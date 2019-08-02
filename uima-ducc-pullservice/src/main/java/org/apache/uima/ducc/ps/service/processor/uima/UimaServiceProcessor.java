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

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.ducc.ps.service.IServiceState;
import org.apache.uima.ducc.ps.service.ServiceConfiguration;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler.Action;
import org.apache.uima.ducc.ps.service.metrics.IWindowStats;
import org.apache.uima.ducc.ps.service.metrics.builtin.ProcessWindowStats;
import org.apache.uima.ducc.ps.service.monitor.IServiceMonitor;
import org.apache.uima.ducc.ps.service.monitor.builtin.RemoteStateObserver;
import org.apache.uima.ducc.ps.service.processor.IProcessResult;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.processor.IServiceResultSerializer;
import org.apache.uima.ducc.ps.service.processor.uima.utils.PerformanceMetrics;
import org.apache.uima.ducc.ps.service.processor.uima.utils.UimaResultDefaultSerializer;
import org.apache.uima.ducc.ps.service.utils.UimaSerializer;
import org.apache.uima.ducc.ps.service.utils.UimaUtils;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.XMLInputSource;

public class UimaServiceProcessor extends AbstractServiceProcessor implements IServiceProcessor {
  public static final String AE_NAME = "AeName";

  public static final String AE_CONTEXT = "AeContext";

  public static final String AE_ANALYSIS_TIME = "AeAnalysisTime";

  public static final String AE_CAS_PROCESSED = "AeProcessedCasCount";

  public static final String IMPORT_BY_NAME_PREFIX = "*importByName:";

  private static String M_PROCESS = "process";

  private static String M_STOP = "stop";

  private static String M_INITIALIZE = "initialize";

  Logger logger = UIMAFramework.getLogger(UimaServiceProcessor.class);

  private IServiceResultSerializer resultSerializer;

  // stores AE instance pinned to a thread
  private ThreadLocal<AnalysisEngine> threadLocal = new ThreadLocal<>();

  private ReentrantLock initStateLock = new ReentrantLock();

  private boolean sendInitializingState = true;

  private int scaleout = 1;

  private String analysisEngineDescriptor;

  private ServiceConfiguration serviceConfiguration;

  private IServiceMonitor monitor;

  private AtomicInteger numberOfInitializedThreads = new AtomicInteger();

  private IServiceErrorHandler errorHandler;

  private Method processMethod;

  private Method stopMethod;

  private Object processorInstance;

  private UimaDelegator uimaDelegator = null;

  public UimaServiceProcessor(String analysisEngineDescriptor) {
    this(analysisEngineDescriptor, new UimaResultDefaultSerializer(), new ServiceConfiguration());
  }

  public UimaServiceProcessor(String analysisEngineDescriptor,
          ServiceConfiguration serviceConfiguration) {
    this(analysisEngineDescriptor, new UimaResultDefaultSerializer(), serviceConfiguration);
  }

  public UimaServiceProcessor(String analysisEngineDescriptor,
          IServiceResultSerializer resultSerializer, ServiceConfiguration serviceConfiguration) {
    this.analysisEngineDescriptor = analysisEngineDescriptor;
    this.resultSerializer = resultSerializer;
    this.serviceConfiguration = serviceConfiguration;
    // start a thread which will collect AE initialization state
    launchStateInitializationCollector();
    if (serviceConfiguration.getJpType() != null) {
      serializerMap = new HashMap<>();
    }
    // check if error window override has been set via -D
    if (serviceConfiguration.getMaxErrors() != null) {
      this.maxErrors = Integer.parseInt(serviceConfiguration.getMaxErrors());
    }
    // check if error window override has been set via -D
    if (serviceConfiguration.getErrorWindowSize() != null) {
      this.windowSize = Integer.parseInt(serviceConfiguration.getErrorWindowSize());
    }
  }

  /*
   * Defines error handling parameters
   * 
   * @param maxErrors - maximum error threshold within an error window
   * 
   * @param windowSize - error window size
   */
  public void setErrorHandlerWindow(int maxErrors, int windowSize) {
    this.maxErrors = maxErrors;
    this.windowSize = windowSize;
  }

  private void launchStateInitializationCollector() {
    monitor = new RemoteStateObserver(serviceConfiguration, logger);
  }

  public void setScaleout(int howManyThreads) {
    this.scaleout = howManyThreads;
  }

  public int getScaleout() {
    return scaleout;
  }

  public void dump(ClassLoader cl, int numLevels) {
    int n = 0;
    for (URLClassLoader ucl = (URLClassLoader) cl; ucl != null
            && ++n <= numLevels; ucl = (URLClassLoader) ucl.getParent()) {
      System.out.println("Class-loader " + n + " has " + ucl.getURLs().length + " urls:");
      for (URL u : ucl.getURLs()) {
        System.out.println("  " + u);
      }
    }
  }

  @Override
  public void initialize() {

    if (logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE,
              "Process Thread:" + Thread.currentThread().getName() + " Initializing AE");

    }
    errorHandler = getErrorHandler(logger);

    try {
      // multiple threads may call this method. Send initializing state once
      initStateLock.lockInterruptibly();
      if (sendInitializingState) {
        sendInitializingState = false; // send init state once
        monitor.onStateChange(IServiceState.State.Initializing.toString(), new Properties());
      }
    } catch (Exception e) {

    } finally {
      initStateLock.unlock();
    }
    // every process thread has its own uima deserializer
    if (serviceConfiguration.getJpType() != null) {
      serializerMap.put(Thread.currentThread().getId(), new UimaSerializer());
    }
    // *****************************************
    // SWITCHING CLASSLOADER
    // *****************************************
    // Ducc code is loaded from jars identified by -Dducc.deploy.DuccClasspath. The jars
    // are loaded into a custom classloader.

    // User code is loaded from jars in the System Classloader. We must switch classloaders
    // if we want to access user code. When user code is done, we restore Ducc classloader to be
    // able
    // to access Ducc classes.

    // Save current context cl and inject System classloader as
    // a context cl before calling user code.
    ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
    boolean failed = false;
    try {

      // For junit testing dont use classpath switching.
      if (Objects.isNull(System.getProperty(CLASSPATH_SWITCH_PROP))) {
        // Running with classpath switching
        Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
        if (logger.isLoggable(Level.FINE)) {
          logger.log(Level.FINE, "",
                  ">>>> initialize():::Context Classloader Switch - Executing code from System Classloader");
        }
        // load proxy class from uima-ducc-user.jar to access uima classes. The UimaWrapper is a
        // convenience/wrapper
        // class so that we dont have to use reflection on Uima classes.
        Class<?> classToLaunch = ClassLoader.getSystemClassLoader()
                .loadClass("org.apache.uima.ducc.user.common.main.UimaWrapper");

        processorInstance = classToLaunch.newInstance();

        Method initMethod = processorInstance.getClass().getMethod(M_INITIALIZE, String.class,
                int.class, boolean.class, ThreadLocal.class);

        processMethod = processorInstance.getClass().getMethod(M_PROCESS,
                new Class[] { String.class, ThreadLocal.class });

        stopMethod = processorInstance.getClass().getMethod(M_STOP, ThreadLocal.class);

        // initialize AE via UimaWrapper
        initMethod.invoke(processorInstance, analysisEngineDescriptor, scaleout,
                (serviceConfiguration.getJpType() != null), threadLocal);

      } else {
        // no classloader switching for junit tests
        ResourceSpecifier rSpecifier = getResourceSpecifier(analysisEngineDescriptor);
        // The UimaDelegator is a convenience class which wraps Uima classes
        uimaDelegator = new UimaDelegator();
        uimaDelegator.initialize(rSpecifier, scaleout, (serviceConfiguration.getJpType() != null),
                threadLocal);
      }

    } catch (Exception e) {
      logger.log(Level.WARNING, null, e);
      failed = true;
      throw new RuntimeException(e);

    } finally {
      Thread.currentThread().setContextClassLoader(savedCL);
      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, "",
                ">>>> initialize():::Context Classloader Switch - Restored Ducc Classloader");
      }
      if (failed) {
        monitor.onStateChange(IServiceState.State.FailedInitialization.toString(),
                new Properties());
      }
    }
    if (logger.isLoggable(Level.INFO)) {
      logger.log(Level.INFO,
              "Process Thread:" + Thread.currentThread().getName() + " Done Initializing AE");

    }
    if (numberOfInitializedThreads.incrementAndGet() == scaleout) {
      // super.delay(logger, DEFAULT_INIT_DELAY);
      monitor.onStateChange(IServiceState.State.Running.toString(), new Properties());
    }
  }

  private ResourceSpecifier getResourceSpecifier(String analysisEngineDescriptor) throws Exception {
    XMLInputSource is = UimaUtils.getXMLInputSource(analysisEngineDescriptor);
    String aed = is.getURL().toString();
    return UimaUtils.getResourceSpecifier(aed);

  }

  @SuppressWarnings("unchecked")
  @Override
  public IProcessResult process(String serializedTask) {
    // save current context cl and inject System classloader as
    // a context cl before calling user code.
    ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
    if (logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE, "",
              ">>>> process():::Context Classloader Switch - Executing code from System Classloader");
    }

    IProcessResult result;

    try {

      // The clients expect metrics in PeformanceMetrics class
      List<PerformanceMetrics> casMetrics;

      // JUnit tests currently dont support CL switching so don't use
      // reflection
      if (Objects.isNull(System.getProperty(CLASSPATH_SWITCH_PROP))) {
        casMetrics = new ArrayList<>();
        // Use basic data structures for returning performance metrics from
        // a processor process(). The PerformanceMetrics class is not
        // visible in the code packaged in uima-ducc-user.jar so return
        // metrics in Properties object for each AE.
        List<Properties> metrics;

        // *****************************************************
        // PROCESS
        // *****************************************************
        metrics = (List<Properties>) processMethod.invoke(processorInstance, serializedTask,
                threadLocal);
        for (Properties p : metrics) {
          // there is Properties object for each AE, so create an
          // instance of PerformanceMetrics and initialize it
          PerformanceMetrics pm = new PerformanceMetrics(p.getProperty(AE_NAME),
                  p.getProperty(AE_CONTEXT), Long.valueOf(p.getProperty(AE_ANALYSIS_TIME)),
                  Long.valueOf(p.getProperty(AE_CAS_PROCESSED)));
          casMetrics.add(pm);
        }
      } else {
        // for JUnit tests call delegator which does not use reflection
        casMetrics = uimaDelegator.process(serializedTask, threadLocal);
      }

      successCount.incrementAndGet();
      errorCountSinceLastSuccess.set(0);
      return new UimaProcessResult(resultSerializer.serialize(casMetrics));
    } catch (Exception e) {
      logger.log(Level.WARNING, "", e);
      IWindowStats stats = new ProcessWindowStats(errorCount.incrementAndGet(), successCount.get(),
              errorCountSinceLastSuccess.incrementAndGet());
      Action action = errorHandler.handleProcessError(e, this, stats);

      result = new UimaProcessResult(e, action);
      return result;
    } finally {
      Thread.currentThread().setContextClassLoader(savedCL);
      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, "",
                ">>>> process():::Context Classloader Switch - Restored Ducc Classloader");
      }
    }
  }

  public void setErrorHandler(IServiceErrorHandler errorHandler) {
    this.errorHandler = errorHandler;
  }

  @Override
  public void stop() {
    logger.log(Level.INFO, this.getClass().getName() + " stop() called");
    // save current context cl and inject System classloader as
    // a context cl before calling user code. This is done in
    // user code needs to load resources
    ClassLoader savedCL = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
    if (logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE, "",
              ">>>> stop():::Context Classloader Switch - Executing code from System Classloader");
    }
    try {
      if (Objects.isNull(System.getProperty(CLASSPATH_SWITCH_PROP))) {
        stopMethod.invoke(processorInstance, threadLocal);
      } else {
        uimaDelegator.stop(threadLocal);
      }
      super.stop();

    } catch (Exception e) {
      logger.log(Level.WARNING, "stop", e);
    } finally {
      Thread.currentThread().setContextClassLoader(savedCL);
      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, "",
                ">>>> stop():::Context Classloader Switch - Restored Ducc Classloader");
      }
    }
  }

}
