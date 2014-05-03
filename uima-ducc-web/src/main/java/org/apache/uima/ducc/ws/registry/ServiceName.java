package org.apache.uima.ducc.ws.registry;

@SuppressWarnings("rawtypes")
public class ServiceName implements Comparable {
	
	private String raw = null;
	
	public ServiceName(String value) {
		raw = value;
	}
	
	public String getRaw() {
		return raw;
	}
	
	public String getNormalized() {
		String normalized = raw;
		if(raw != null) {
			if(raw.contains("?")) {
				normalized = raw.substring(0, raw.indexOf("?"));
			}
		}
		return normalized;
	}
    
	@Override
	public String toString() {
		return getNormalized();
	}
	
	@Override
    public int compareTo(Object value)
    { 
        if ( value instanceof ServiceName ) {
            return getNormalized().compareTo(((ServiceName) value).getNormalized());
        } else {
            return this.compareTo(value);
        }
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getNormalized() == null) ? 0 : getNormalized().hashCode());
		return result;
	}
	
}
