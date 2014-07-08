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

    String textColor = "white";
    String fillColor = "black";

    JobFragment(String user, DuccType type, String id, int mem, int qshares, String service_endpoint)
    {
        this.user             = user;
        this.type             = type;
        this.id               = id;
        this.qshares          = qshares;
        this.mem              = mem;
        this.nprocesses       = 1;
        this.service_endpoint = service_endpoint;
        setColors();
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

    /**
     * Set the fill and text color based on a hash of the user.
     */
    void setColors ()
    {
        if ( type == DuccType.Undefined ) {
            fillColor = "0,0,0";
            textColor = "256,256,256";
        } else {
            int color_index = (user + " ").hashCode();  // we add " " because orginal viz did and this keeps the colors consistent.

            color_index = Math.abs(color_index) % 512;
            int r = (color_index % 8)        * 28 + 44;
            int g = ((color_index / 8) % 8)  * 28 + 44;
            int b = ((color_index / 64) % 8) * 28 + 44;
            if (r + g + b < 60) {
                r *=2 ; g *=2; b *=2;
            }

            int brightness = (int)Math.sqrt(
                              r * r * .241 + 
                              g * g * .691 + 
                              b * b * .068);

            fillColor = "rgb(" + r + "," + g + "," + b + ")";
            textColor = ( brightness < 130 ? "white" : "black" );
        }
//    
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
}

