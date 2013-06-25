package org.apache.uima.ducc.common;

import java.io.Serializable;

/**
 * The ServiceStatics class is used to return service health, availability, and monitoring statistics
 * to the Service Manager.
 */
public interface  IServiceStatistics
    extends Serializable
{

    /**
     * Query whether the service is alive.
     * @return "true" if the service is responsive, "false" otherwise.
     */
    public boolean isAlive();

    /**
     * Query wether the service is "healthy".
     * @return "true" if the service is healthy, "false" otherwise.
     */
    public boolean isHealthy();

    /**
     * Return service statistics, if any.
     * @return A string containing information regarding the service.  This is used only for display in the web server.
     */
    public String  getInfo();

    /**
     * Set the "aliveness" of the service.  This is called by each pinger for each service.
     * @param alive Set to "true" if the service is responseve, "false" otherwise.
     */
    public void setAlive(boolean alive);
 
    /**
     * Set the "health" of the service.  This is called by each pinger for each service.
     * @param healthy Set to "true" if the service is healthy, "false" otherwise.
     */
    public void setHealthy(boolean healthy);

    /**
     * Set the monitor statistics for the service.  This is called by each pinger for each service.
     * @param info This is an arbitrary string summarizing the service's performance.  This is used only in the web serverl
     */
    public void setInfo(String info);

}
