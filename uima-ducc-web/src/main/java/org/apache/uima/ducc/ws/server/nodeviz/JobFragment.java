package org.apache.uima.ducc.ws.server.nodeviz;

import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;

    /**
     * Represents the number of processes for a given job, and the total qshares occupied.
     */
class JobFragment
{

    String   user;             // Owner of the job
    String   id;               // DUCC id of the job
    int      nprocesses;       // number of processes in the fragment (i.e. on the same node)
    int      qshares;          // total qshares represented by the node
    String   service_endpoint; // for services only, the defined endpoint we we can link back to services page
    int      mem;              // Actual memory requested
    String   color;            // color to draw this
    DuccType type;             // Job, Service, Reservation, Pop. 

    JobFragment(String user, DuccType type, String id, int mem, int qshares, String service_endpoint)
    {
        this.user             = user;
        this.type             = type;
        this.id               = id;
        this.qshares          = qshares;
        this.mem              = mem;
        this.nprocesses       = 1;
        this.service_endpoint = service_endpoint;
    }

    void addShares(int qshares)
    {
        this.qshares += qshares;
        this.nprocesses++;
    }

    boolean matches(String id)
    {
        return this.id.equals(id);
    }

    String getTitle() { 
        switch ( type ) {
            case Reservation:
                return id;
            case Undefined:
                return "";
            default:
                return id + " " + nprocesses + " processes"; 
        }
    }
    String getUser() { return user; }
}

