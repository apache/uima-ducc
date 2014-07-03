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
    
    void svgStart(float height, float width)
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
		out.append("<rect x=\""
				+ (x*XSCALE) 
				+ "\" y=\""
				+ (y*YSCALE)
				+ "\" width=\""
				+ (width*XSCALE)
				+ "\" height=\""
				+ (height*YSCALE)
				+ "\"  fill=\"" 
				+ color 
				+ "\" stroke=\"" 
				+ borderColor 
				+ "\" stroke-width=\"" 
				+ (strokeWidth*XSCALE) 
				+ "\"" 
				+ newAttr
				+ "/> ");
	}
    
	void nodeLabel(float x, float y, String label)
    {
		out.append(
				"<text x=\"" 
						+ (x*XSCALE) 
						+ "\" y=\""
						+ (y*YSCALE) 
						+ "\" font-family=\"helvetica\" font-size=\"10\" font-weight=\"bold\"" 
						+ " fill=\"black\">" 
						+ label
						+ "</text>"
				);
	}

	void text(float x, float y, String label, String color, float fontsize, String newAttr) 
    {
		if (newAttr == null) { 
			newAttr = "";
		}
		out.append(
				"<text x=\"" 
						+ (x*XSCALE) 
						+ "\" y=\""
						+ (y*YSCALE) 
						+ "\" font-family=\"verdana\" font-size=\"" 
						+ fontsize
						+ "\"  fill=\"" 
						+ color 
						+ "\"" 
						+ newAttr
						+ ">"
						+ label
						+ "</text>"
				);
	}

    void hyperlinkStart(VisualizedHost host, JobFragment j)
    {
        switch ( j.type ) {
            case Job:
                out.append(
                           "<a xlink:href=job.details.jsp?id=" +
                           j.id + 
                           ">"
                           );

                break;
            case Service:
                out.append(
                           "<a xlink:href=/service.details.jsp?name=" +
                           j.service_endpoint +
                           ">"
                           );

                break;
            case Pop:
                out.append(
                           "<a xlink:href=/reservations.details.jsp?id=" +
                           j.id + 
                           ">"
                           );

                break;
            case Reservation:
                out.append(
                           "<a xlink:href=/reservations.jsp>"
                          );
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

                out.append(
                           "<title>" +
                           jobtype +
                           " " +
                           j.user +
                           ":" +
                           j.id +
                           " runs " +
                           j.nprocesses +
                           " process(es) of " +
                           j.mem +
                           "GB on host " +
                           h.name + 
                           "(" +
                           h.mem + 
                           "GB)</title>" 
                           );
                break;
            case Reservation:
                out.append(
                           "<title>" +
                           jobtype +
                           " " +
                           j.user +
                           ":" +
                           j.id +                           
                           " on host " +
                           h.name +
                           "(" +
                           j.mem +
                           "GB)</title>"
                          );
                break;
            case Undefined:
                out.append(
                           "<title>" +
                           j.qshares + 
                           " unused shares (" +
                           (j.qshares * NodeViz.quantum) +
                           "GB) on " +
                           h.name + 
                           "(" +
                           h.mem +
                           "GB)</title>"
                           );
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

