package org.apache.uima.ducc.ws.server.nodeviz;



public class Markup
{

    private int XSCALE = 8;
    private int YSCALE = 8;

    private StringBuffer out;

    public Markup()
    {
        this.out = new StringBuffer();
    }

    String close()
    {
    	return out.toString();
    }
    
    void svgStart(float width, float height)
    {
        out.append("<svg xmlns=\"http://www.w3.org/2000/svg\" height=\"" + (height * YSCALE) + "\" width=\"" + (width * XSCALE) + "\">" );
    }

    void svgEnd()
    {
        out.append("</svg>");
    }

    void divStart()
    {
        // out.append("<div style=\"border: 1px solid red;display:inline-block\">");
        // out.append("<div style=\"border: 1px solid red;display:inline-block;vertical-align:baseline\">");
        out.append("<div style=\"display:inline-block\">");
    }

    void divEnd()
    {
        out.append("</div>");
    }

	void tooltipStart(String label) {
		out.append("<g>\n<title>" + label + "</title>");
	}

	void tooltipEnd() {
		out.append("</g>");
	}

	void rect(float x, float y, float width, float height, String color, String borderColor, float strokeWidth, String newAttr) 
    {
		if (newAttr == null) { 
			newAttr = "";
		}
		out.append("<rect x=\"");
        out.append((x*XSCALE) );
        out.append("\" y=\"");
        out.append((y*YSCALE));
        out.append("\" width=\"");
        out.append((width*XSCALE));
        out.append("\" height=\"");
        out.append((height*YSCALE));
        out.append("\"  fill=\"" );
        out.append(color );
        out.append("\" stroke=\"" );
        out.append(borderColor );
        out.append("\" stroke-width=\"" );
        out.append((strokeWidth*XSCALE) );
        out.append("\"" );
        out.append(newAttr);
        out.append("/> ");
	}
    
	void nodeLabel(float x, float y, String label)
    {
		out.append("<text x=\"");
        out.append((x*XSCALE));
        out.append("\" y=\"");
        out.append((y*YSCALE) );
        out.append("\" font-family=\"helvetica\" font-size=\"10\" font-weight=\"bold\"" );
        out.append(" fill=\"black\">" );
        out.append(label);
        out.append("</text>");
	}

	void text(float x, float y, String label, String color, int fontsize)
    {
		out.append("<text x=\"");
        out.append(Float.toString(x * XSCALE));
        out.append("\" y=\"");
        out.append(Float.toString(y * YSCALE));
        out.append("\" font-family=\"verdana\" font-size=\"");
        out.append(Integer.toString(fontsize));
        out.append("\"  fill=\"");
        out.append(color);
        out.append("\"");
        out.append(">");
        out.append(label);
        out.append("</text>");
	}

    void hyperlinkStart(VisualizedHost host, JobFragment j)
    {
        switch ( j.type ) {
            case Job:
                out.append("<a xlink:href=job.details.jsp?id=");
                out.append(j.id);
                out.append(">");
                break;

            case Service:
                out.append("<a xlink:href=/service.details.jsp?name=");
                out.append(j.service_endpoint);
                out.append(">");
                break;

            case Pop:
                out.append("<a xlink:href=/reservations.details.jsp?id=");
                out.append(j.id);
                out.append(">");

                break;
            case Reservation:
                out.append("<a xlink:href=/reservations.jsp>");
                break;

            case Undefined:
                out.append("<a>");
                break;
        }
    }
    
    void hyperlinkEnd()
    {
        out.append("</a>");
    }

    void titleForFragment(VisualizedHost h, JobFragment j)
    {
        String jobtype = "" + j.type;
        switch ( j.type ) {
            case Pop:
                jobtype = "Managed Reservation";
            case Job:
            case Service:

                out.append("<title>");
                out.append(jobtype);
                out.append(" ");
                out.append(j.user);
                out.append(":");
                out.append(j.id);
                out.append(" runs ");
                out.append(j.nprocesses);
                out.append(" process(es) of ");
                out.append(j.mem);
                out.append("GB on host ");
                out.append(h.name); 
                out.append("(");
                out.append(h.mem); 
                out.append("GB)</title>");
                break;
            case Reservation:
                out.append("<title>");
                out.append(jobtype);
                out.append(" ");
                out.append(j.user);
                out.append(":");
                out.append(j.id);                           
                out.append(" on host ");
                out.append(h.name);
                out.append("(");
                out.append(j.mem);
                out.append("GB)</title>");
                break;
            case Undefined:
                out.append("<title>");
                out.append(j.qshares); 
                out.append(" unused shares (");
                out.append((j.qshares * NodeViz.quantum));
                out.append("GB) on ");
                out.append(h.name); 
                out.append("(");
                out.append(h.mem);
                out.append("GB)</title>");
                break;
        }
    }

    String patternedFill(JobFragment j)
    {
        String color = j.fillColor;

        switch ( j.type ) {
            case Job:
                return color;
            case Pop:
                return popFill(j, color);
            case Service:
            	return serviceFill(j, color);
            case Reservation:
                return reservationFill(j, color);
            default:
            	return color;
        }
    }

	String popFill(JobFragment j, String color)
    {
        String id = "patP" + j.id;
        
        out.append("<pattern id=\"");
        out.append(id);
        out.append("\" patternUnits=\"userSpaceOnUse\" x=\"0\" y=\"0\" width=\"4\" height=\"4\">");
        out.append("<g>");

        out.append("<line x1=\"-2\" y1=\"4\" x2=\"4\" y2=\"-2\" stroke=\"");
        out.append(color);
        out.append("\" stroke-width=\"2\" />");

        out.append("<line x1=\"0\" y1=\"6\" x2=\"6\" y2=\"0\" stroke=\"");
        out.append(color);
        out.append("\" stroke-width=\"2\" />");

        out.append("</g>");
        out.append("</pattern>");

        return "url(#" + id + ")";
	}

    String serviceFill(JobFragment j, String color)
    {
        String id = "patS" + j.id;

        out.append("<pattern id=\"");
        out.append(id);
        out.append("\" patternUnits=\"userSpaceOnUse\" x=\"0\" y=\"0\" width=\"4\" height=\"4\">");
        out.append("<g>");

        out.append("<line x1=\"0\" y1=\"-2\" x2=\"6\" y2=\"4\" stroke=\"");
        out.append(color);
        out.append("\" stroke-width=\"2\" />");

        out.append("<line x1=\"-2\" y1=\"0\" x2=\"4\" y2=\"6\" stroke=\"");
        out.append(color);
        out.append("\" stroke-width=\"2\" />");

        out.append("</g>");
        out.append("</pattern>");

        return "url(#" + id + ")";
    }

    String reservationFill(JobFragment j, String color)
    {
        String id = "patR" + j.id;

        out.append("<pattern id=\"");
        out.append(id);
        out.append("\" patternUnits=\"userSpaceOnUse\" x=\"0\" y=\"0\" width=\"4\" height=\"4\">");
        out.append("<g><rect x=\"0\" y=\"0\" width=\"3.7\" height=\"3.7\" style=\"fill:");
        out.append(color);
        out.append("; stroke:none\"/></g></pattern>");
        return "url(#" + id + ")";
    }

}

