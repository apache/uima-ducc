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
     * Query whether the service is alive. This is used internally by the Service Manager.
     *
     * @return "true" if the service is responsive, "false" otherwise.
     */
    public boolean isAlive();

    /**
     * Query wether the service is "healthy". This is used internally by the Service Manager.
     * @return "true" if the service is healthy, "false" otherwise.
     */
    public boolean isHealthy();

    /**
     * Return service statistics, if any. This is used internally by the Service Manager.
     * @return A string containing information regarding the service. 
     */
    public String  getInfo();

    /**
     * Set the "aliveness" of the service.  This is called by each pinger for each service.  Set
     *  this to return "true" if the service is responsive.  Otherwise return "false" so the Service
     *  Manager can reject jobs dependent on this service.
     * @param alive Set to "true" if the service is responseve, "false" otherwise.
     */
    public void setAlive(boolean alive);
 
    /**
     * Set the "health" of the service.  This is called by each pinger for each service.  This is a
     * subject judgement made by the service owner on behalf of his own service.  This is used only
     * to reflect status in the DUCC Web Server.
     * @param healthy Set to "true" if the service is healthy, "false" otherwise.
     */
    public void setHealthy(boolean healthy);

    /**
     * Set the monitor statistics for the service. This is any arbitray string describing critical
     * or useful characteristics of the service.  This string is presented as a "hover" in the
     * webserver over the "health" field.
     * @param info This is an arbitrary string summarizing the service's performance. 
     */
    public void setInfo(String info);

}
