package org.apache.uima.ducc.sm;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;


class TcpStreamHandler
    extends URLStreamHandler
{
    TcpStreamHandler() {}
    
    public URLConnection openConnection(URL u)
    {
        //throw new Exception("This protocol handler isn't expected to actually work.");
        return null;
    }
}
