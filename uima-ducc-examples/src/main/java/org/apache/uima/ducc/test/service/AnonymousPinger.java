package org.apache.uima.ducc.test.service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

import org.apache.uima.ducc.cli.AServicePing;
import org.apache.uima.ducc.cli.ServiceStatistics;


/**
 * This is designed for the simple sleeper "service" that does nothing other than
 * wait for requests from the pinger.
 *
 * The necessary endpoint is CUSTOM:name:host:port
 */
public class AnonymousPinger
    extends AServicePing
{
    String host;
    int  port;

    public void init(String arguments, String endpoint)
    	throws Exception
    {
        System.out.println("INIT: endpoint " + endpoint);
        System.out.println("Socket file: " + arguments);

        FileInputStream fis = new FileInputStream(arguments);

        byte[] bytes = new byte[128];
        int bytesread = fis.read(bytes);
        fis.close();
        String sockloc = new String(bytes, 0, bytesread);
        System.out.println("Service is listening at " + sockloc);

        String [] parts = sockloc.split(":");
        for ( String s : parts ) {
        	System.out.println("Parts: " + s);
        }
        host = parts[0];
        port = Integer.parseInt(parts[1]);
        System.out.println("Parsed service location to " + host + " : " + port);
    }

    public void stop()
    {

    }

    public ServiceStatistics getStatistics()
    {
        ServiceStatistics stats = new ServiceStatistics(false, false,"<NA>");
        Socket sock = null;

        try {
            sock = new Socket(host, port);
            
            ObjectInputStream dis = new ObjectInputStream(sock.getInputStream());
            String response = (String) dis.readObject();
            System.out.println("Pong response: " + response);

            stats.setAlive(true);
            stats.setHealthy(true);
            stats.setInfo( response );
        } catch ( Throwable t) {
        	t.printStackTrace();
            stats.setInfo(t.getMessage());
        } finally {
        	try { sock.close(); } catch (IOException e) {}
        }
        return stats;        
    }

    public static void main(String[] args)
    {
        try {
			AnonymousPinger cp = new AnonymousPinger();
			cp.init(args[0], args[1]);
			for ( int i = 0; i < 10; i++ ) {
			    ServiceStatistics stats = cp.getStatistics();
			    System.out.println(stats);
			    Thread.sleep(2000);
			}
			cp.stop();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

}
