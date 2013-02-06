package org.apache.uima.ducc.common;

import java.io.Serializable;

public class  ServiceStatistics
    implements Serializable
{
	private static final long serialVersionUID = 1L;
	private boolean alive = false;
    private boolean healthy = false;
    private String info = "N/A";
    private String errorString = null;

    public ServiceStatistics(boolean alive, boolean healthy, String info)
    {
        this.alive = alive;
        this.healthy = healthy;
        this.info = info;
    }

    public boolean isAlive()   { return alive; }            // is the service active and functioning ?
    public boolean isHealthy() { return healthy; }          // is the service healthy ?
    public String  getInfo()   { return info; }             // additional service-specific information

    /**
     * If there were problems, this returns a string for some sort of presentation or logging.
     *
     * Be sure to update this to null if the object is reused after returning errors.
     */
    public String getErrorString()
    {
        return errorString;
    }
    
    public void setAlive(boolean alive)
    {
        this.alive = alive;
    }

    public void setHealthy(boolean healthy)
    {
        this.healthy = healthy;
    }

    public void setInfo(String info)
    {
        this.info = info;
    }

    public void setErrorString(String err)
    {
        this.errorString = err;
    }

    public String toString()
    {
        if ( errorString == null ) {
            return "Alive[" + alive + "] Healthy[" + healthy + "] + Info: " + info;
        } else {
            return "Alive[" + alive + "] Healthy[" + healthy + "] + Errors: " + errorString;
        }
    }

}
