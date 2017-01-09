/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.ducc.tools;


import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.google.gson.Gson;


public class DuccPubListener
    implements MessageListener
{
    String host;
    int port;
    String topic_str;
    String[] agents;
    String outfn = null;
    int    generation = 0;
    int    timeout = 0;
    Map<String, Boolean> agentPresence = new HashMap<String, Boolean>();
    int total_agents = 0;
    
    Connection connection;
    Session session;
    Topic topic;

    boolean do_filter = false;

    public DuccPubListener (String host, int port, String topic_str, int timeout, String outfn, String[] agents)
    {
        this.host = host;
        this.port = port;
        this.topic_str = topic_str;
        this.timeout = timeout;
        this.outfn = outfn;
        this.agents = agents;

        for ( String a : agents ) {
            agentPresence.put(a, false);
        }
        total_agents = agentPresence.size();

        if ( ((topic_str.indexOf("metrics") >= 0 ) || (topic_str.indexOf("inventory") >= 0) ) 
             && ( total_agents > 0 ) ) {
            do_filter = true;
        }
            
    }

    protected void connect()
        throws Throwable
    {
        String url = "tcp://" + host + ":" + port;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic(topic_str);

        MessageConsumer consumer = session.createConsumer(topic);
        consumer.setMessageListener(this);

        connection.start();

        if ( timeout > 0 ) {
            try {
                Thread.sleep(timeout * 1000);
            } catch ( Throwable t ){
            }
            close();
        }
    }

    protected void close()
     	throws Throwable
    {
        connection.close();
		System.exit(0);
    }

    /**
     * For agents - we might be looking for multiple messages. Need to search the message to see if
     * one of the watch agent names is there.
     */
    String receivedAgent(String msg)
    {
        String answer = null;

        for ( String a : agents ) {
            if ( agentPresence.get(a) == true ) {
                continue;
            }
            if ( msg.indexOf(a) >= 0 ) {
                agentPresence.put(a, true);
                total_agents--;
                answer = a;
            }
        }

        return answer;
    }

    void writeString(String fn, String msg)
    {
        try {
			FileWriter fw = new FileWriter(fn);
			fw.write(msg);
			fw.close();
			System.out.println(fn);          // for filters to know what was written
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    public void onMessage(Message message) 
    {
        // System.out.println("Got message");
        try {
            if ( message instanceof ObjectMessage ) {

                String suffix = null;
                Object obj = ((ObjectMessage)message).getObject();     
                Gson gson = new Gson();
                String msg = gson.toJson(obj);

                suffix = ".json";
                if ( !do_filter ) {
                    String out = outfn + suffix;
                    if ( timeout > 0 ) out = out + "." + (++generation);
                    writeString(out, msg);
                    if ( timeout <= 0 ) close();
                } else {
                    String agent = receivedAgent(msg);                    
                    if ( agent != null ) {                        
                        String out = outfn + "." + agent + suffix;
                        writeString(out, msg);
                    }

                    if ( total_agents == 0 ) close();
                }
            }
		} catch (Throwable t) {
            t.printStackTrace();
		}
    }

    public void run()
    {
        try {
			connect();
		} catch (Throwable e) {
			e.printStackTrace();
		}
    }

    static void usage(String msg)
    {
        if ( msg != null ) System.out.println(msg);

        System.out.println("Usage:");
        System.out.println("   DuccPubListener args");
        System.out.println("Where args are:");
        System.out.println("   -host AMQhostname");
        System.out.println("         Default is \"localhost\"");
        System.out.println("   -port AMQportname");
        System.out.println("         Default is 61616");
        System.out.println("   -topic topic");
        System.out.println("         No default, required");
        System.out.println("   -output outputFileName");
        System.out.println("         Default is topic");
        System.out.println("   -timeout timeInSeconds");
        System.out.println("         How long to listen.  Default is 0.  If 0, exit after first message.");
        System.out.println("   -agent agentname");
        System.out.println("         For agent broadcasts, which agent to listen for. Default is \"all\"");

        System.exit(1);
    }

    public static void main(String[] args)
    {

        if ( args.length == 0 ) usage(null);

        String[] agents = {"all"};
        String amqhost = "localhost";
        int    amqport = 61616;
        int    timeout = 0;
        String topic = null;
        String outfn = null;

        for ( int i = 0; i < args.length; ) {

            if ( "-agent".startsWith(args[i]) ) {
                agents = args[i+1].split(",");
                i++; i++;
                continue;
            }

            if ( "-host".startsWith(args[i]) ) {
                amqhost = args[i+1];
                i++; i++;
                continue;
            }

            if ( "-port".startsWith(args[i]) ) {
                try {
                    amqport = Integer.parseInt(args[i+1]);
                } catch ( Throwable t ) {
                    usage("AMQ port is not numeric");
                }
                i++; i++;
                continue;
            }

            if ( "-topic".startsWith(args[i]) ) {
                topic = args[i+1];
                i++; i++;
                continue;
            }

            if ( "-timeout".startsWith(args[i]) ) {
                try {
                    timeout = Integer.parseInt(args[i+1]);
                } catch ( Throwable t ) {
                    usage("Timeout is not numeric");
                }
                i++; i++;
                continue;
            }


            if ( "-output".startsWith(args[i]) ) {
                outfn = args[i+1];
                i++; i++;
                continue;
            }

            usage(null);
        }

        if ( topic == null ) {
            usage("Must specify topic");
        }

        if ( outfn == null ) {
            outfn = topic;
        }

        DuccPubListener dl = new DuccPubListener(amqhost, amqport, topic, timeout, outfn,  agents);
        dl.run();
    }
}

