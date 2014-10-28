package org.apache.uima.ducc.container.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DuccLogger implements IDuccLogger {

	static public DuccLogger getLogger(@SuppressWarnings("rawtypes") Class clazz, String component) {
		return new DuccLogger(clazz, component);
	}
	
	private static enum Type { 
		DEBUG(true), INFO(true), WARN(true), ERROR(true);
		private boolean active = true;
		private Type(boolean value) {
			active = value;
		}
		public boolean isActive() {
			return(active);
		}
	};
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
	
	public DuccLogger(@SuppressWarnings("rawtypes") Class clazz, String component) {
		setClazz(clazz);
		setComponent(component);
	}
	
	private String clazz = null;
	private String component = null;
	
	private boolean displayComponent = false;
	
	private void setClazz(Class<?> value) {
		clazz = value.getSimpleName();
	}

	private void setComponent(String value) {
		component = value;
	}
	
	private String format(Object...args) {
		StringBuffer text = new StringBuffer();
		if(args != null) {
			for(Object arg : args) {
				if(arg != null) {
					if(arg instanceof String) {
						text.append(arg);
					}
					else {
						text.append(arg.toString());
					}
					text.append(" ");	
				}
			}
		}
		return text.toString().trim();
	}
	
	private void log(Type type, String location, IDuccId jobid, Object... args) {
		StringBuffer text = new StringBuffer();
		Date date = Calendar.getInstance().getTime();
		text.append(sdf.format(date));
		text.append(" ");
		text.append(type.name());
		text.append(" ");
		if(displayComponent) {
			text.append(component);
			text.append(":");
		}
		text.append(clazz);
		text.append(".");
		text.append(location);
		text.append(" ");	
		text.append(format(args));
		System.out.println(text);
	}
	
	@Override
	public void debug(String location, IDuccId jobid, Object... args) {
		if(Type.DEBUG.isActive()) {
			log(Type.DEBUG, location, jobid, args);
		}
	}
	
	@Override
	public void error(String location, IDuccId jobid, Object... args) {
		if(Type.ERROR.isActive()) {
			log(Type.ERROR, location, jobid, args);
		}
	}
	
	@Override
	public void info(String location, IDuccId jobid, Object... args) {
		if(Type.INFO.isActive()) {
			log(Type.INFO, location, jobid, args);
		}
	}
}
