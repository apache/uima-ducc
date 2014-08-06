package org.apache.uima.ducc.ws.server;

import java.util.Properties;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceState;
import org.apache.uima.ducc.ws.registry.IServicesRegistry;

public class DuccServicesState {

	private static DuccLogger duccLogger = DuccLoggerComponents.getWsLogger(DuccHandlerUtils.class.getName());
	private static DuccId duccId = null;
	
	private static String valueTrue = "True";
	
	private static enum Health { health_black, health_red, health_green };
	
	private static String makeDisplayValue(String state, String health, String popupText) {
		String location = "makeDisplayValue";
		String retVal = "?";
		try {
			StringBuffer sb = new StringBuffer();
			sb.append("<span ");
			if(popupText != null) {
				sb.append("title=\"");
				sb.append(popupText);
				sb.append("\"");
				sb.append(" ");
			}
			sb.append("class=\"");
			if(health != null) {
				sb.append(health);
			}
			else {
				sb.append(Health.health_black);
			}
			sb.append("\"");
			sb.append(">");
			if(state != null) {
				sb.append(state);
			}
			else {
				sb.append("?");
			}
			sb.append("</span>");
			retVal = sb.toString();
		}
		catch(Exception e) {
			duccLogger.error(location, duccId, e);
		}
		return retVal;
	}
	
	private enum PingerStatus { PingerInactive };
	private enum HealthStatus { HealthPoor };
	
	private static boolean isPingerActive(Properties propertiesMeta) {
		boolean retVal = false;
		String pinger = getUninterpreted(propertiesMeta, IServicesRegistry.ping_active);
		String value = pinger.trim();
		if(value.equalsIgnoreCase(valueTrue)) {
			retVal = true;
		}
		return retVal;
	}
	
	private static void getHealthStatus(StringBuffer sb, Properties propertiesMeta) {
		String location = "getHealthStatus";
		try {
			String health = getUninterpreted(propertiesMeta, IServicesRegistry.service_healthy);
			String value = health.trim();
			if(!value.equalsIgnoreCase(valueTrue)) {
				sb.append(HealthStatus.HealthPoor.name()+"\n");
			}
		}
		catch(Exception e) {
			duccLogger.error(location, duccId, e);
		}
	}
	
	private static void getErrorsStatus(StringBuffer sb, Properties propertiesMeta) {
		String location = "getErrorsStatus";
		try {
			String error = getUninterpreted(propertiesMeta, IServicesRegistry.submit_error);
			String value = error.trim();
			if(value.length() > 0) {
				sb.append(value+"\n");
			}
		}
		catch(Exception e) {
			duccLogger.error(location, duccId, e);
		}
	}
	
	private static void getStatistics(StringBuffer sb, Properties propertiesMeta) {
		String location = "getErrorsStatus";
		try {
			String statistics = propertiesMeta.getProperty(IServicesRegistry.service_statistics);
			String value = statistics.trim();
			if(value.length() > 0) {
				sb.append(value.replaceAll("\"", "")+"\n");
			}
		}
		catch(Exception e) {
			duccLogger.error(location, duccId, e);
		}
	}
	
	private static String getAlerts(Properties propertiesMeta) {
		StringBuffer sb = new StringBuffer();
		getHealthStatus(sb, propertiesMeta);
		getErrorsStatus(sb, propertiesMeta);
		String retVal = sb.toString();
		return retVal;
	}
	
	private static String getErrors(Properties propertiesMeta) {
		StringBuffer sb = new StringBuffer();
		getErrorsStatus(sb, propertiesMeta);
		String retVal = sb.toString();
		return retVal;
	}
	
	private static String getStatistics(Properties propertiesMeta) {
		StringBuffer sb = new StringBuffer();
		getStatistics(sb, propertiesMeta);
		String retVal = sb.toString();
		return retVal;
	}
	
	private static boolean isHealthIrrelevant(String state) {
		boolean retVal = false;
		if(state.equalsIgnoreCase(ServiceState.Starting.name())) {
			retVal = true;
		}
		else if(state.equalsIgnoreCase(ServiceState.Waiting.name())) {
			retVal = true;
		}
		else if(state.equalsIgnoreCase(ServiceState.Initializing.name())) {
			retVal = true;
		}
		else if(state.equalsIgnoreCase(ServiceState.Stopped.name())) {
			retVal = true;
		}
		return retVal;
	}
	
	private static String getPopup(String state, Properties propertiesMeta) {
		String retVal = "The service is "+state;
		if(state.equalsIgnoreCase(ServiceState.Waiting.name())) {
			retVal = "Pinger is starting";
			/*
			String type = getUninterpreted(propertiesMeta, IServicesRegistry.service_type);
			type = type.trim();
			if(type.equalsIgnoreCase("CUSTOM")) {
				retVal = "Pinger is starting";
			}
			else {
				retVal = "Pinger and Service are starting";
			}
			*/
		}
		return retVal;
	}
	
	public static String getServiceState(Properties propertiesMeta) {
		String location = "getServiceState";
		String retVal = "?";
		String value = "?";
		try {	
			String state = getUninterpreted(propertiesMeta, IServicesRegistry.service_state);
			value = state.trim();
			// Stopped
			if(isHealthIrrelevant(value)) {
				String errors = getErrors(propertiesMeta);
				if(errors.length() > 0) {
					String popup = errors;
					retVal = makeDisplayValue(value+"+Alert", Health.health_red.toString(), popup);
				}
				else {
					String popup = getPopup(state, propertiesMeta);
					retVal = makeDisplayValue(value, Health.health_black.toString(), popup);
				}
			}
			// PingerInactive
			else if(!isPingerActive(propertiesMeta)) {
				String popup = PingerStatus.PingerInactive.name()+"\n";
				retVal = makeDisplayValue(value+"+Alert", Health.health_red.toString(), popup);
			}
			else {
				String alerts = getAlerts(propertiesMeta);
				String statistics = getStatistics(propertiesMeta);
				String popup = alerts+statistics;
				// Alert (other than PingerInactive)
				if(alerts.length() > 0) {
					retVal = makeDisplayValue(value+"+Alert", Health.health_red.toString(), popup);
				}
				else {
					// Available
					if(value.equalsIgnoreCase(ServiceState.Available.name())) {
						retVal = makeDisplayValue(value, Health.health_green.toString(), popup);
					}
					// Transient state (to/from Available)
					else {
						retVal = makeDisplayValue(value, Health.health_black.toString(), popup);
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
