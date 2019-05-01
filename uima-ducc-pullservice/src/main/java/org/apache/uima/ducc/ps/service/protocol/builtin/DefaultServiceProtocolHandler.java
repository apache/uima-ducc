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
package org.apache.uima.ducc.ps.service.protocol.builtin;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.uima.UIMAFramework;
import org.apache.uima.ducc.ps.net.iface.IMetaTask;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Type;
import org.apache.uima.ducc.ps.net.impl.MetaTaskTransaction;
import org.apache.uima.ducc.ps.net.impl.TransactionId;
import org.apache.uima.ducc.ps.service.IService;
import org.apache.uima.ducc.ps.service.errors.IServiceErrorHandler.Action;
import org.apache.uima.ducc.ps.service.errors.ServiceException;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.processor.IProcessResult;
import org.apache.uima.ducc.ps.service.processor.IServiceProcessor;
import org.apache.uima.ducc.ps.service.protocol.INoTaskAvailableStrategy;
import org.apache.uima.ducc.ps.service.protocol.IServiceProtocolHandler;
import org.apache.uima.ducc.ps.service.transport.IServiceTransport;
import org.apache.uima.ducc.ps.service.transport.TransportException;
import org.apache.uima.ducc.ps.service.transport.XStreamUtils;
import org.apache.uima.ducc.ps.service.utils.Utils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

import com.thoughtworks.xstream.XStream;

/**
 * 
 * This protocol handler is a Runnable
 * 
 */
public class DefaultServiceProtocolHandler implements IServiceProtocolHandler {
  Logger logger = UIMAFramework.getLogger(DefaultServiceProtocolHandler.class);

  private volatile boolean initError = false;

  private volatile boolean running = false;

  private volatile boolean quiescing = false;

  private IServiceTransport transport;

  private IServiceProcessor processor;

  private INoTaskAvailableStrategy noTaskStrategy;

  // each process thread will count down the latch after intialization
  private CountDownLatch initLatch;

  // this PH will count the stopLatch down when it is about to stop. The service
  // is the owner of this latch and awaits termination blocking in start()
  private CountDownLatch stopLatch;

  // each process thread block on startLatch until application calls start()
  private CountDownLatch startLatch;

  // reference to a service so that stop() can be called
  private IService service;

  // forces process threads to initialize serially
  private static ReentrantLock initLock = new ReentrantLock();

  private ReentrantLock noWorkLock = new ReentrantLock();

  private static AtomicInteger idGenerator = new AtomicInteger();

  private Thread retryThread = null;

  // Create ThreadLocal Map containing instances of XStream for each thread
  private ThreadLocal<HashMap<Long, XStream>> threadLocalXStream = new ThreadLocal<HashMap<Long, XStream>>() {
    @Override
    protected HashMap<Long, XStream> initialValue() {
      return new HashMap<>();
    }
  };

  private DefaultServiceProtocolHandler(Builder builder) {
    this.initLatch = builder.initLatch;
    this.stopLatch = builder.stopLatch;
    this.service = builder.service;
    this.transport = builder.transport;
    this.processor = builder.processor;
    this.noTaskStrategy = builder.strategy;
  }

  private void waitForAllThreadsToInitialize() {
    try {
      initLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

  }

  private void initialize() throws ServiceInitializationException {
    // this latch blocks all process threads after initialization
    // until application calls start()
    startLatch = new CountDownLatch(1);
    try {
      // use a lock to serialize initialization one thread at a time
      initLock.lock();
      if (initError) {
        return;
      }
      processor.initialize();
    } catch (Throwable e) {
      initError = true;
      running = false;
      logger.log(Level.WARNING, "ProtocolHandler initialize() failed -", e);
      throw new ServiceInitializationException(
              "Thread:" + Thread.currentThread().getName() + " Failed initialization - " + e);
    } finally {

      initLatch.countDown();
      initLock.unlock();
      if (!initError) {
        // wait on startLatch
        waitForAllThreadsToInitialize();
      }
    }
  }

  public boolean initialized() {
    return (initError == false);
  }

  private IMetaTaskTransaction sendAndReceive(IMetaTaskTransaction transaction) throws Exception {
    TransactionId tid;
    if (Type.Get.equals(transaction.getType())) {
      int major = idGenerator.addAndGet(1);
      int minor = 0;

      tid = new TransactionId(major, minor);
    } else {
      tid = transaction.getTransactionId();
      // increment minor
      tid.next();
    }

    transaction.setRequesterProcessName(service.getType());
    transport.addRequestorInfo(transaction);
    IMetaTaskTransaction reply = null;
    try {
      // XStream is thread safe so multiple threads can serialize concurrently
      // String body = XStreamUtils.marshall(transaction);
      String body = threadLocalXStream.get().get(Thread.currentThread().getId()).toXML(transaction);
      // dispatch implements waiting if no task is given by the driver
      reply = transport.dispatch(body, threadLocalXStream);

      if (Objects.isNull(reply)) {
        throw new TransportException(
                "Received invalid content (null) in response from client - rejecting request");
      }

    } catch (Exception e) {
      if (!running) {
        throw new TransportException("Service stopping - rejecting request");
      }
      throw e;
    }
    return reply;
  }

  private IMetaTaskTransaction callEnd(IMetaTaskTransaction transaction) throws Exception {
    transaction.setType(Type.End);
    if (logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE, "ProtocolHandler calling END");
    }
    return sendAndReceive(transaction);

  }

  private IMetaTaskTransaction callAck(IMetaTaskTransaction transaction) throws Exception {
    transaction.setType(Type.Ack);
    if (logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE, "ProtocolHandler calling ACK");
    }
    return sendAndReceive(transaction);
  }

  /**
   * Fetch new task from a remote driver.
   * 
   * When the driver is out of tasks, a single thread first sleeps for awhile and than tries again
   * until a task is returned.
   * 
   * @param transaction
   * @return
   * @throws Exception
   */
  private IMetaTaskTransaction callGet(IMetaTaskTransaction transaction) throws Exception {
    transaction.setType(Type.Get);
    if (logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE, "ProtocolHandler calling GET");
    }
    IMetaTaskTransaction metaTransaction = null;

    while (running) {
      metaTransaction = sendAndReceive(transaction);
      if (metaTransaction.getMetaTask() != null
              && metaTransaction.getMetaTask().getUserSpaceTask() != null) {
        return metaTransaction;
      }

      // If the first thread to get the lock poll for work and unlock when work found
      // If don't immediately get the lock then wait for the lock to be released when
      // work becomes available,
      // and immediately release the lock and loop back to retry
      boolean firstLocker = noWorkLock.tryLock();
      if (!firstLocker) {
        noWorkLock.lock();
        noWorkLock.unlock();
        continue;
      }

      // If the first one here hold the lock and sleep before retrying
      if (logger.isLoggable(Level.INFO)) {
        logger.log(Level.INFO, "Driver is out of tasks - waiting for "
                + noTaskStrategy.getWaitTimeInMillis() + "ms before trying again ");
      }
      while (running) {
        noTaskStrategy.handleNoTaskSupplied();
        metaTransaction = sendAndReceive(transaction);
        if (metaTransaction.getMetaTask() != null
                && metaTransaction.getMetaTask().getUserSpaceTask() != null) {
          noWorkLock.unlock();
          return metaTransaction;
        }
      }
    }
    ;

    return metaTransaction; // When shutting down
  }

  /**
   * Block until service start() is called
   * 
   * @throws ServiceInitializationException
   */
  private void awaitStart() throws ServiceInitializationException {
    try {
      startLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceInitializationException("Thread interrupted while awaiting start()");
    }
  }

  public String call() throws ServiceInitializationException, ServiceException {
    // we may fail in initialize() in which case the ServiceInitializationException
    // is thrown
    initialize();

    // now wait for application to call start
    awaitStart();

    // check ThreadLocal for a Map entry for this thread id. If not found, create
    // dedicated XStream instance for this thread which will be useed to serialize/deserialize
    // this thread's tasks
    if (threadLocalXStream.get().get(Thread.currentThread().getId()) == null) {
      threadLocalXStream.get().put(Thread.currentThread().getId(),
              XStreamUtils.getXStreamInstance());// new XStream(new DomDriver()));
    }
    // all threads intialized, enter running state

    IMetaTaskTransaction transaction = null;

    if (logger.isLoggable(Level.INFO)) {
      logger.log(Level.INFO,
              ".............. Thread " + Thread.currentThread().getId() + " ready to process");
    }

    while (running) {

      try {
        // send GET Request
        transaction = callGet(new MetaTaskTransaction());
        // the code may have blocked in callGet for awhile, so check
        // if service is still running. If this service is in quiescing
        // mode, finish processing current task. The while-loop will
        // terminate when the task is finished.
        if (!running && !quiescing) {
          break;
        }
        // transaction may be null if retryUntilSuccessfull was interrupted
        // due to stop
        if (Objects.isNull(transaction) || (!running && !quiescing)) {
          break;
        }
        logger.log(Level.FINE,
                ".............. Thread " + Thread.currentThread().getId() + " processing new task");
        if (Objects.isNull(transaction.getMetaTask())) {
          // this should only be the case when the service is stopping and transport is
          // shutdown
          if (running) {
            logger.log(Level.INFO, ".............. Thread " + Thread.currentThread().getId()
                    + " GET returned null MetaTask while service is in a running state - this is unexpected");
          }
          // if !running, the while loop above will terminate
          continue;
        }

        Object task = transaction.getMetaTask().getUserSpaceTask();

        // send ACK
        transaction = callAck(transaction);
        if (!running && !quiescing) {
          break;
        }
        IProcessResult processResult = processor.process((String) task);

        // assume success
        Action action = Action.CONTINUE;
        // check if process error occurred.
        String errorAsString = processResult.getError();

        if (processResult.terminateProcess()) {
          action = Action.TERMINATE;
        } else if (Objects.isNull(errorAsString)) {
          // success
          transaction.getMetaTask().setPerformanceMetrics(processResult.getResult());
        }
        if (Objects.nonNull(errorAsString)) {
          IMetaTask mc = transaction.getMetaTask();
          // the ducc.deploy.JpType is only present for jobs. If not specified
          // we return stringified exception to the client. The JD expects
          // Java Exception object for its error handling
          if (Objects.isNull(System.getProperty("ducc.deploy.JpType"))) {

            mc.setUserSpaceException(errorAsString);
          } else {
            logger.log(Level.INFO, "Sending Exception to JD:\n"
                    + ((Exception) processResult.getExceptionObject()));
            // JD expects serialized exception as byte[]
            mc.setUserSpaceException(serializeError(processResult.getExceptionObject()));
          }

        }

        // send END Request
        callEnd(transaction);
        if (running && Action.TERMINATE.equals(action)) {
          logger.log(Level.WARNING, "Processor Failure - Action=Terminate");
          // Can't stop using the current thread. This thread
          // came from a thread pool we want to stop. Need
          // a new/independent thread to call stop()
          new Thread(new Runnable() {

            @Override
            public void run() {
              delegateStop();
            }
          }).start();
          running = false;
        }

      } catch (IllegalStateException e) {
        break;
      } catch (TransportException e) {
        break;
      } catch (Exception e) {
        logger.log(Level.WARNING, "", e);
        break;
      }
    }
    stopLatch.countDown();
    System.out.println(Utils.getTimestamp() + ">>>>>>> " + Utils.getShortClassname(this.getClass())
            + ".call() >>>>>>>>>> Thread [" + Thread.currentThread().getId() + "] "
            + " ProtocolHandler stopped requesting new tasks - Stopping processor");
    logger.log(Level.INFO, "ProtocolHandler stopped requesting new tasks - Stopping processor");

    if (processor != null) {
      processor.stop();
    }
    return String.valueOf(Thread.currentThread().getId());
  }

  private byte[] serializeError(Throwable t) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);

    try {
      oos.writeObject(t);
    } catch (Exception e) {
      try {
        logger.log(Level.WARNING,
                "Unable to Serialize " + t.getClass().getName() + " - Will Stringify It Instead");

      } catch (Exception ee) {
      }
      throw e;
    } finally {
      oos.close();
    }

    return baos.toByteArray();
  }

  private void delegateStop() {
    service.quiesceAndStop();
  }

  @Override
  public void stop() {
    quiescing = false;
    running = false;
    try {
      // use try catch to handle a possible race condition
      // when retryThread is not null, but it becomes null
      // before we call interrupt causing NPE. All this would
      // mean is that retryUntilSuccess() succeeded.
      if (retryThread != null) {
        retryThread.interrupt();
      }
    } catch (Exception ee) {
    } // noTaskStrategy.interrupt();
    if (logger.isLoggable(Level.INFO)) {
      logger.log(Level.INFO, this.getClass().getName() + " stop() called");
    }
  }

  @Override
  public void quiesceAndStop() {

    // Use System.out since the logger's ShutdownHook may have closed streams
    System.out.println(Utils.getTimestamp() + ">>>>>>> " + Utils.getShortClassname(this.getClass())
            + ".queisceAndStop()");
    logger.log(Level.INFO, this.getClass().getName() + " quiesceAndStop() called");
    // change state of transport to not running but keep connection open
    // so that other threads can quiesce (send results)
    transport.stop(true);

    quiescing = true;
    running = false;
    try {
      // use try catch to handle a possible race condition
      // when retryThread is not null, but it becomes null
      // before we call interrupt causing NPE. All this would
      // mean is that retryUntilSuccess() succeeded.
      if (retryThread != null) {
        retryThread.interrupt();
      }
    } catch (Exception ee) {
    }
    try {
      // wait for process threads to terminate
      stopLatch.await();
    } catch (Exception e) {

    }
    // Use System.out since the logger's ShutdownHook may have closed streams
    System.out.println(Utils.getTimestamp() + ">>>>>>> " + Utils.getShortClassname(this.getClass())
            + ".queisceAndStop() All process threads completed quiesce");
    logger.log(Level.INFO, this.getClass().getName() + " All process threads completed quiesce");
  }

  @Override
  public void start() {
    running = true;
    // process threads are initialized and are awaiting latch countdown
    startLatch.countDown();
  }

  @Override
  public void setServiceProcessor(IServiceProcessor processor) {
    this.processor = processor;
  }

  @Override
  public void setTransport(IServiceTransport transport) {
    this.transport = transport;
  }

  public static class Builder {
    private IServiceTransport transport;

    private IServiceProcessor processor;

    private INoTaskAvailableStrategy strategy;

    // each thread will count down the latch
    private CountDownLatch initLatch;

    private CountDownLatch stopLatch;

    private IService service;

    public Builder withTransport(IServiceTransport transport) {
      this.transport = transport;
      return this;
    }

    public Builder withProcessor(IServiceProcessor processor) {
      this.processor = processor;
      return this;
    }

    public Builder withInitCompleteLatch(CountDownLatch initLatch) {
      this.initLatch = initLatch;
      return this;
    }

    public Builder withDoneLatch(CountDownLatch stopLatch) {
      this.stopLatch = stopLatch;
      return this;
    }

    public Builder withNoTaskStrategy(INoTaskAvailableStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public Builder withService(IService service) {
      this.service = service;
      return this;
    }

    public DefaultServiceProtocolHandler build() {
      return new DefaultServiceProtocolHandler(this);
    }
  }
}
