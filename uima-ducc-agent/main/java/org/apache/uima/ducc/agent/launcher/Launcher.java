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
package org.apache.uima.ducc.agent.launcher;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.uima.ducc.agent.NodeAgent;
import org.apache.uima.ducc.agent.event.ProcessLifecycleObserver;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.cmdline.JavaCommandLine;
import org.apache.uima.ducc.transport.event.common.DuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;


public class Launcher {
    private ExecutorService executorService = null;
    private DuccIdFactory duccIdFactory = new DuccIdFactory();
    public Launcher() {
    	executorService = Executors.newCachedThreadPool();
    }
    /**
     * This method is used to launch multiple Agents on the same physical machine. It allows
     * to scale up Agents on a single machine to simulate load. Each Agent instance will 
     * assume a given name and IP address.
     * 
     * @param cmdLine - defines the command line used to exec Agent process
     * @param howMany - how many Agents to exec
     * @param ip - initial IP address to assign to first Agent instance. Subsequent Agent instances
     *             will have this IP incremented as in XXX.XXX.XXX.XX+1,XXX.XXX.XXX.XX+2, etc
     * @param nodeName - name of the physical machine. The Agents will be assigned this name plus 
     *                   a suffix as in watson-1, watson-2, etc
     * @throws Exception
     */
    public void start(ICommandLine cmdLine, int howMany, String ip, String nodeName) throws Exception {
        //	Launch as many agents as requested  
        for( int i=0; i < howMany; i++ ) {
            String host = new String(nodeName);
            //	Append suffix to node name for each Agent instance
            if ( host.indexOf(".") > -1 ) {
                String tmp = host.substring(0,host.indexOf("."));
                host = tmp+"-"+String.valueOf(i+1)+host.substring(host.indexOf("."));
            } else {
                host = host+"-"+String.valueOf(i+1);
            }
            launchProcess(host, ip, cmdLine);
            //	Increment IP address for the next Agent
            int uip = Integer.parseInt(ip.substring(ip.lastIndexOf(".")+1, ip.length()));
            ip = ip.substring(0,ip.lastIndexOf(".")+1).concat(String.valueOf(uip+1));
        }

    }
	
    /**
     * Submit request to exec a process. The process will be exec'd in a separate thread.
     * 
     * @param process
     * @param commandLine

     * @throws Exception
     */
    public ManagedProcess launchProcess(NodeAgent agent, NodeIdentity nodeIdentity,IDuccProcess process, ICommandLine commandLine,ProcessLifecycleObserver observer, ManagedProcess managedProcess) 
    		throws Exception {
        //	Instantiate executor that will actually exec the process using java's ProcessBuilder
        DuccCommandExecutor executor = 
            new DuccCommandExecutor(agent, commandLine, nodeIdentity.getName(),nodeIdentity.getIp(), managedProcess);
        Future<?> future = executorService.submit(executor);
        //	if we are launching a process, save the future object returned from Executor above
    	managedProcess.setFuture(future);
        return managedProcess;
    }
    /**
     * This method is used to testing only. It enables launching an agent with 
     * a given name and IP address which are different from a physical node name
     * and IP address. With that multiple agents can be launched on the same 
     * physical machine simulating a cluster of nodes.
     * 
     */
    public void launchProcess(String host, String ip, ICommandLine cmdLine) throws Exception {
        IDuccProcess process = 
            new DuccProcess(duccIdFactory.next(), new NodeIdentity(ip, host));
        process.setProcessType(ProcessType.Pop);
        ManagedProcess managedProcess = new ManagedProcess(process, cmdLine, true);
        DuccCommandExecutor executor = 
            new DuccCommandExecutor(cmdLine, host, ip, managedProcess);
        executorService.submit(executor);
    }
    public static void main(String[] args) {
        try {
            int howMany = Integer.parseInt(args[0]);   // how many agent processes to launch
            String ip = System.getProperty("IP");
            String nodeName = InetAddress.getLocalHost().getHostName();
            Launcher launcher = new Launcher();
            JavaCommandLine cmdLine = new JavaCommandLine("java");
            String duccHome = System.getenv("DUCC_HOME");
            cmdLine.addOption("-Dducc.deploy.configuration="+duccHome+"/resources/ducc.properties");
            cmdLine.addOption("-Dducc.deploy.components=agent");
            cmdLine.addOption("-Djava.library.path=" + duccHome +"/lib/sigar");
            cmdLine.addOption("-DDUCC_HOME=" + duccHome);
			
            // System.out.println("Spawning with classpath: \n" + System.getProperty("java.class.path"));
            // cmdLine.setClasspath(duccHome+"/lib/*:" + 
            //                      duccHome+"/lib/apache-activemq-5.5.0/*:" + 
            //                      duccHome+"/lib/apache-camel-2.7.1/*:" + 
            //                      duccHome+"/lib/commons-collections-3.2.1/*:" + 
            //                      duccHome+"/lib/apache-commons-lang-2.6/*:" + 
            //                      duccHome+"/lib/apache-log4j-1.2.16/*:" + 
            //                      duccHome+"/lib/guava-r09/*:" + 
            //                      duccHome+"/lib/joda-time-1.6/*:" + 
            //                      duccHome+"/lib/sigar/*:" + 
            //                      duccHome+"/lib/springframework-3.0.5/*:");

            cmdLine.setClasspath(System.getProperty("java.class.path"));
            cmdLine.setClassName("org.apache.uima.ducc.agent.common.main.DuccService");
            launcher.start(cmdLine, howMany, ip, nodeName);
        } catch( Exception e) {
            e.printStackTrace();
        }
    }

}
