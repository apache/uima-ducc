package org.apache.uima.ducc.cli;

import org.apache.uima.ducc.common.IServiceStatistics;



/**
 * The ServiceStatics class is used to return service health, availability, and monitoring statistics
 * to the Service Manager.
 */
public class  ServiceStatistics
    implements IServiceStatistics
{
	private static final long serialVersionUID = 1L;
	private boolean alive = false;
    private boolean healthy = false;
    private String info = "N/A";

    /**
     * Consstructor
     *
     * @param alive This indicates whether the service is responsive to the pinger.  Set to "true" if so, and
     *              to false otherwise.  If set "false", the Service Manager assumes the service is not
     *              available and will not allow new work dependent on it to start.
     *
     * @param healthy This indicates whether the service is responding adequately.  This is purely subjective,
     *                and is determined by each individul ping implementation.  The information is used
     *                only for display in the web server.
     *
     * @param info This is a string containing monitoring information about the service.  It is used only
     *             for display in the web server.
     */
    public ServiceStatistics(boolean alive, boolean healthy, String info)
    {
        this.alive = alive;
        this.healthy = healthy;
        this.info = info;
    }

    /**
     * Query whether the service is alive.
     * @return "true" if the service is responsive, "false" otherwise.
     */
    public boolean isAlive()   { return alive; }            // is the service active and functioning ?

    /**
     * Query wether the service is "healthy".
     * @return "true" if the service is healthy, "false" otherwise.
     */
    public boolean isHealthy() { return healthy; }          // is the service healthy ?

    /**
     * Return service statistics, if any.
     * @return A string containing information regarding the service.  This is used only for display in the web server.
     */
    public String  getInfo()   { return info; }             // additional service-specific information

    /**
     * Set the "aliveness" of the service.  This is called by each pinger for each service.
     * @param alive Set to "true" if the service is responseve, "false" otherwise.
     */
    public void setAlive(boolean alive)
    {
        this.alive = alive;
    }
 
    /**
     * Set the "health" of the service.  This is called by each pinger for each service.
     * @param healthy Set to "true" if the service is healthy, "false" otherwise.
     */
   public void setHealthy(boolean healthy)
    {
        this.healthy = healthy;
    }

    /**
     * Set the monitor statistics for the service.  This is called by each pinger for each service.
     * @param info This is an arbitrary string summarizing the service's performance.  This is used only in the web serverl
     */
    public void setInfo(String info)
    {
        this.info = info;
    }

    /**
     * A simple formatter for the class
     */
    public String toString()
    {
        return "Alive[" + alive + "] Healthy[" + healthy + "] + Info: " + info;
    }

}
