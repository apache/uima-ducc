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

package org.apache.uima.ducc.ps.sd;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.UIMAFramework;

import org.apache.uima.ducc.ps.sd.iface.ServiceDriver;
import org.apache.uima.ducc.ps.sd.task.DuccServiceTaskProtocolHandler;
import org.apache.uima.ducc.ps.sd.task.TestTaskAllocatorCallbackListener;
import org.apache.uima.ducc.ps.sd.task.error.TaskProtocolException;
import org.apache.uima.ducc.ps.sd.task.iface.TaskAllocatorCallbackListener;
import org.apache.uima.ducc.ps.sd.task.iface.TaskProtocolHandler;
import org.apache.uima.ducc.ps.sd.task.transport.TaskTransportException;
import org.apache.uima.ducc.ps.sd.task.transport.Transports;
import org.apache.uima.ducc.ps.sd.task.transport.Transports.TransportType;
import org.apache.uima.ducc.ps.sd.task.transport.iface.TaskTransportHandler;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction;
import org.apache.uima.ducc.ps.net.iface.IMetaTaskTransaction.Type;
import org.apache.uima.ducc.ps.net.impl.MetaTaskTransaction;
import org.apache.uima.ducc.ps.net.impl.TransactionId;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class DuccServiceDriver implements ServiceDriver {
  Logger logger = UIMAFramework.getLogger(DuccServiceDriver.class);

  private TaskAllocatorCallbackListener taskAllocator;

  private TaskTransportHandler transport;

  private TransportType transportType = TransportType.HTTP;

  private TaskProtocolHandler protocolHandler = null;

  // prevent empty constructor instantiation
  private DuccServiceDriver() {
  }

  /**
   * Thread safe singleton helper class
   */
  private static class DuccServiceDriverSingleton {
    private static final DuccServiceDriver instance = new DuccServiceDriver();
  }

  public static DuccServiceDriver getInstance() {
    return DuccServiceDriverSingleton.instance;
  }

  public TaskAllocatorCallbackListener getTaskAllocator() {
    return taskAllocator;
  }

  public void setTaskAllocator(TaskAllocatorCallbackListener taskAllocator) {
    this.taskAllocator = taskAllocator;
  }

  public String start() throws Exception {
    if (protocolHandler == null) {
      throw new DriverException("start() called before initialize()");
    }
    if (transport == null) {
      throw new DriverException("start() called before initialize()");
    }
    if (taskAllocator == null) {
      throw new DriverException("start() called before setTaskAllocator()");
    }
    String retValue = protocolHandler.start();
    transport.start();

    return retValue;
  }

  public void stop() throws Exception {
    if (transport != null) {
      transport.stop();
    }
    if (protocolHandler != null) {
      protocolHandler.stop();
    }
  }

  public String initialize(Properties properties) throws DriverException {
    if (TransportType.HTTP.equals(transportType)) {
      transport = Transports.newHttpTransport();

      protocolHandler = new DuccServiceTaskProtocolHandler(taskAllocator);
      try {
        logger.log(Level.INFO, "Initializing protocol handler ...");
        protocolHandler.initialize(properties);
        logger.log(Level.INFO, "Initializing transport ...");
        transport.setTaskProtocolHandler(protocolHandler);
        return transport.initialize(properties);
      } catch (TaskProtocolException e) {
        throw new DriverException(e);
      } catch (TaskTransportException e) {
        throw new DriverException(e);
      } catch (Exception e) {
        throw new DriverException(e);
      }
    }
    return null;
  }

  public void test() throws Exception {
    AtomicInteger IdGenerator = new AtomicInteger();
    IMetaTaskTransaction transaction = new MetaTaskTransaction();

    int major = IdGenerator.addAndGet(1);
    int minor = 0;

    TransactionId tid = new TransactionId(major, minor);
    transaction.setTransactionId(tid);
    // According to HTTP spec, GET may not contain Body in
    // HTTP request. HttpClient actually enforces this. So
    // do a POST instead of a GET.
    transaction.setType(Type.Get); // Tell JD you want a Work Item
    protocolHandler.handle(transaction);
    logger.log(Level.INFO, "Returned from Get Handler - Client Got Message");
    transaction.setType(Type.Ack); // Tell JD you want a Work Item
    protocolHandler.handle(transaction);
    logger.log(Level.INFO, "Returned from Ack Handler - Client Got Message");
    transaction.setType(Type.End); // Tell JD you want a Work Item
    protocolHandler.handle(transaction);
    logger.log(Level.INFO, "Returned from End Handler - Client Got Message");

  }

  public static void main(String[] args) {
    String port = "8888";
    String application = "/test";
    if (args.length > 0) {
      if (args.length != 2) {
        System.out.println("Two arguments required: port application (defaults: 8888 /test");
        return;
      }
      port = args[0];
      application = args[1];
    }
    try {
      Properties properties = new Properties();
      properties.put(ServiceDriver.Port, port);
      properties.put(ServiceDriver.Application, application);
      properties.put(ServiceDriver.MaxThreads, "100");

      TaskAllocatorCallbackListener taskAllocator = new TestTaskAllocatorCallbackListener();
      ServiceDriver driver = DuccServiceDriver.getInstance();
      driver.setTaskAllocator(taskAllocator);
      driver.initialize(properties);
      driver.start();
      // ((DuccServiceDriver)driver).test();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
