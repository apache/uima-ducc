package org.apache.uima.ducc.ws.server;

import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;

public class DuccServicesState {

	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerUtils.class.getName());
	private static DuccId duccId = null;
	
	private static String valueTrue = "True";
	private static String valueStopped = "Stopped";

	private static enum Health { health_black, health_red, health_green };
	
	private static enum State { Unknown, Error, Stopped, Good, Poor };
	
	private static String popupPingerDown = "Pinger down";
	
	private static String makeDisplayValue(State state, Health health, String popupText) {
		String location = "makeDisplayValue";
		String retVal = "?";
		try {
			StringBuffer sb = new StringBuffer();
			sb.append("<span title=\"");
			sb.append(popupText);
			sb.append("\"");
			sb.append(" ");
			sb.append("class=\"");
			sb.append(health.name());
			sb.append("\"");
			sb.append(">");
			sb.append(state.name());
			sb.append("</span>");
			retVal = sb.toString();
		}
		catch(Exception e) {
			duccLogger.error(location, duccId, e);
		}
		return retVal;
	}
	
	public static String getServiceState(Properties propertiesMeta) {
		String location = "getServiceState";
		String retVal = "?";
		String value = "?";
		try {
			if(propertiesMeta.containsKey(IServicesRegistry.submit_error)) {
				String popup = propertiesMeta.getProperty(IServicesRegistry.submit_error);
				retVal = makeDisplayValue(State.Error, Health.health_red, popup);
			}
			else {
				String state = getUninterpreted(propertiesMeta, IServicesRegistry.service_state);
				value = state.trim();
				if(value.equalsIgnoreCase(valueStopped)) {
					String popup = "";
					retVal = makeDisplayValue(State.Stopped, Health.health_black, popup);
				}
				else {
					String pinger = getUninterpreted(propertiesMeta, IServicesRegistry.ping_active);
					value = pinger.trim();
					if(value.equalsIgnoreCase(valueTrue)) {
						String health = getUninterpreted(propertiesMeta, IServicesRegistry.service_healthy);
						value = health.trim();
						String popup = propertiesMeta.getProperty(IServicesRegistry.service_statistics);
						if(value.equalsIgnoreCase(valueTrue)) {
							retVal = makeDisplayValue(State.Good, Health.health_green, popup);
						}
						else {
							retVal = makeDisplayValue(State.Poor, Health.health_red, popup);
						}
					}
					else {
						retVal = makeDisplayValue(State.Unknown, Health.health_black, popupPingerDown);
					}
				}
			}
		}
		catch(Exception e) {
			duccLogger.error(location, duccId, e);
		}
		return retVal;
	}
	
	public static String getUninterpreted(Properties propertiesMeta, String key) {
		String retVal = "";
		if(propertiesMeta != null) {
			if(key != null) {
				if(propertiesMeta.containsKey(key)) {
					String value = propertiesMeta.getProperty(key);
					if(value != null) {
						retVal = value.trim();
					}
				}
			}
		}
		return retVal;
	}
}
