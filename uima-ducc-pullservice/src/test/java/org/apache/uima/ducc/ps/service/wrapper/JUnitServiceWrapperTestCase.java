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
package org.apache.uima.ducc.ps.service.wrapper;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.uima.ducc.ps.Client;
import org.apache.uima.ducc.ps.StateMonitor;
import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.ducc.ps.service.main.ServiceWrapper;
import org.junit.Test;

public class JUnitServiceWrapperTestCase extends Client {
  private static final long DELAY = 5000;
  {
    // static initializer sets amount of time the service delays
    // sending READY to a monitor
    System.setProperty("ducc.service.init.delay", "3000");
  }

  @Test
  public void testPullServiceWrapperNoTask() throws Exception {
    // make client return null task in response to GET
    System.setProperty("simulate.no.work", "3");
    System.setProperty("ducc.process.thread.sleep.time", "1000");
    try {
      testPullServiceWrapper();
    } finally {
      System.getProperties().remove("simulate.no.work");
    }
  }

  @Test
  public void testPullServiceWrapper() throws Exception {
    System.out.println("-------------------------- testPullServiceWrapper ----------------------");
    ;

    // int scaleout = 2;
    StateMonitor monitor = new StateMonitor();
    monitor.start();
    System.out.println("........... Monitor Port:" + System.getProperty("DUCC_STATE_UPDATE_PORT"));
    super.startJetty(false); // don't block
    String analysisEngineDescriptor = "TestAAE";
    System.setProperty("ducc.deploy.JpType", "uima");

    String tasURL = "http://localhost:" + super.getPort() + "/test";
    try {
      System.setProperty("ducc.deploy.JdURL", tasURL);
      System.setProperty("ducc.deploy.JpThreadCount", "12");
      System.setProperty("ducc.deploy.service.type", "NotesService");
      System.setProperty("ducc.deploy.JpType", "uima");

      ServiceWrapper service = new ServiceWrapper();

      Timer fTimer = new Timer("testPullService Timer");
      // after 5secs stop the pull service
      fTimer.schedule(new MyTimerTask(service, fTimer), 40000);

      service.initialize(new String[] { analysisEngineDescriptor });

      service.start();

    } catch (ServiceInitializationException e) {
      throw e;
    } catch (Exception e) {
      throw e;
    } finally {
      monitor.stop();
      super.stopJetty();

    }
  }

  @Test
  public void testPullServiceWrapperWithProcessFailure() throws Exception {
    System.out.println(
            "-------------------------- testPullServiceWrapperWithProcessFailure ----------------------");
    ;
    // int scaleout = 2;
    StateMonitor monitor = new StateMonitor();
    monitor.start();
    System.out.println("........... Monitor Port:" + System.getProperty("DUCC_STATE_UPDATE_PORT"));
    super.startJetty(false); // don't block
    String analysisEngineDescriptor = "NoOpAE";

    String tasURL = "http://localhost:" + super.getPort() + "/test";
    try {
      // Force process failure of the first task
      System.setProperty("ProcessFail", "1");

      System.setProperty("ducc.deploy.JdURL", tasURL);
      System.setProperty("ducc.deploy.JpThreadCount", "4");
      System.setProperty("ducc.deploy.service.type", "NotesService");
      System.setProperty("ducc.deploy.JpType", "uima");
      // use default error window (1,1)
      ServiceWrapper service = new ServiceWrapper();

      Timer fTimer = new Timer("testPullService Timer");
      // after 5secs stop the pull service
      fTimer.schedule(new MyTimerTask(service, fTimer), 10000);

      service.initialize(new String[] { analysisEngineDescriptor });

      service.start();

    } catch (ServiceInitializationException e) {
      throw e;
    } catch (Exception e) {
      throw e;
    } finally {
      monitor.stop();
      System.getProperties().remove("ProcessFail");
      super.stopJetty();
    }
  }

  @Test
  public void testPullServiceWrapperDDGenerator() throws Exception {
    System.out.println(
            "-------------------------- testPullServiceWrapperDDGenerator ----------------------");
    ;

    // int scaleout = 2;
    StateMonitor monitor = new StateMonitor();
    monitor.start();
    System.out.println("........... Monitor Port:" + System.getProperty("DUCC_STATE_UPDATE_PORT"));
    super.startJetty(false); // don't block
    // Dont change the name of TestAAE.xml. This is setup to fail file lookup and force
    // generation of AE descriptor.
    String analysisEngineDescriptor = "TestAAE.xml";
    System.setProperty("ducc.deploy.JpType", "uima");

    String tasURL = "http://localhost:" + super.getPort() + "/test";
    try {

      System.setProperty("ducc.deploy.JdURL", tasURL);
      System.setProperty("ducc.deploy.JpThreadCount", "4");
      System.setProperty("ducc.deploy.service.type", "NotesService");
      System.setProperty("ducc.deploy.JpType", "uima");
      System.setProperty("ducc.deploy.JpAeDescriptor", "NoOpAE");
      System.setProperty("ducc.deploy.JobDirectory", System.getProperty("user.dir"));
      System.setProperty("ducc.deploy.JpFlowController",
              "org.apache.uima.flow.FixedFlowController");
      System.setProperty("ducc.process.log.dir", System.getProperty("user.dir"));
      System.setProperty("ducc.job.id", "2000");
      ServiceWrapper service = new ServiceWrapper();

      Timer fTimer = new Timer("testPullService Timer");
      // after 5secs stop the pull service
      fTimer.schedule(new MyTimerTask(service, fTimer), 20000);

      service.initialize(new String[] { analysisEngineDescriptor });

      service.start();

    } catch (ServiceInitializationException e) {
      throw e;
    } catch (Exception e) {
      throw e;
    } finally {
      monitor.stop();
      super.stopJetty();
      File directory = new File(
              System.getProperty("user.dir").concat("/").concat(System.getProperty("ducc.job.id")));

      if (directory.exists()) {
        for (File f : directory.listFiles()) {
          if (f.getName().startsWith("uima-ae-")) {
            f.delete();
            System.out.println("Removed generated descriptor:" + f.getAbsolutePath());
          }
        }
        directory.delete();

      }

    }
  }

  class MyTimerTask extends TimerTask {
    final ServiceWrapper service;

    final Timer fTimer;

    MyTimerTask(ServiceWrapper service, Timer fTimer) {
      this.service = service;
      this.fTimer = fTimer;
    }

    @Override

    public void run() {
      this.cancel();
      fTimer.purge();
      fTimer.cancel();
      System.out.println("Timmer popped - stopping service");
      service.stop();

    }

  }

}
