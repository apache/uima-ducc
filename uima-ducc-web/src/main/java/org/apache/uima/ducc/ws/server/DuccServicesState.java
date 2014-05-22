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
	
	private static enum State { Starting, Initializing, WaitingForPinger, WaitingForService, Available, AvailablePoor, AvailableNotPinging, Stopping, Stopped, Error };
	
	private static String popupStopped = "The service is stopped";
	private static String popupStopping = "The service is stopping";
	private static String popupPingerDown = "The service pinger is not reporting";
	private static String popupStarting = "The service is starting";
	private static String popupInitializing = "The service has started and is now initializing";
	private static String popupInitialized = "The service has initialized";
	
	private static String makeDisplayValue(State state, Health health, String popupText) {
		return makeDisplayValue(state.name(), health.name(), popupText);
	}
	
	private static String makeDisplayValue(String state, String health, String popupText) {
		String location = "makeDisplayValue";
		String retVal = "?";
		try {
			StringBuffer sb = new StringBuffer();
			sb.append("<span title=\"");
			sb.append(popupText);
			sb.append("\"");
			sb.append(" ");
			sb.append("class=\"");
			sb.append(health);
			sb.append("\"");
			sb.append(">");
			sb.append(state);
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
			// Error
			if(propertiesMeta.containsKey(IServicesRegistry.submit_error)) {
				String popup = propertiesMeta.getProperty(IServicesRegistry.submit_error);
				retVal = makeDisplayValue(State.Error, Health.health_red, popup);
			}
			else {
				String state = getUninterpreted(propertiesMeta, IServicesRegistry.service_state);
				value = state.trim();
				// Stopped
				if(value.equalsIgnoreCase(ServiceState.Stopped.name())) {
					retVal = makeDisplayValue(State.Stopped, Health.health_black, popupStopped);
				}
				// Stopping
				else if(value.equalsIgnoreCase(ServiceState.Stopping.name())) {
					retVal = makeDisplayValue(State.Stopping, Health.health_black, popupStopping);
				}
				// Available
				else if(value.equalsIgnoreCase(ServiceState.Available.name())) {
					String pinger = getUninterpreted(propertiesMeta, IServicesRegistry.ping_active);
					value = pinger.trim();
					if(value.equalsIgnoreCase(valueTrue)) {
						String health = getUninterpreted(propertiesMeta, IServicesRegistry.service_healthy);
						value = health.trim();
						String popupStats = propertiesMeta.getProperty(IServicesRegistry.service_statistics);
						if(popupStats == null) {
							popupStats = "";
						}
						popupStats = popupStats.replaceAll("\"", "");
						if(value.equalsIgnoreCase(valueTrue)) {
							// Available (Good)
							retVal = makeDisplayValue(State.Available, Health.health_green, popupStats);
						}
						else {
							// AvailablePoor
							retVal = makeDisplayValue(State.AvailablePoor, Health.health_red, popupStats);
						}
					}
					else {
						// AvailableNotPinging
						retVal = makeDisplayValue(State.AvailableNotPinging, Health.health_red, popupPingerDown);
					}
				}
				// Waiting
				else if(value.equalsIgnoreCase(ServiceState.Waiting.name())) {
					String pinger = getUninterpreted(propertiesMeta, IServicesRegistry.ping_active);
					value = pinger.trim();
					if(value.equalsIgnoreCase(valueTrue)) {
						retVal = makeDisplayValue(State.WaitingForService, Health.health_black, popupInitialized);
					}
					else {
						retVal = makeDisplayValue(State.WaitingForPinger, Health.health_black, popupInitialized);
					}
				}
				else if(value.equalsIgnoreCase(ServiceState.Initializing.name())) {
					retVal = makeDisplayValue(State.Initializing, Health.health_black, popupInitializing);
				}
				else if(value.equalsIgnoreCase(ServiceState.Starting.name())) {
					retVal = makeDisplayValue(State.Starting, Health.health_black, popupStarting);
				}
				else {
					retVal = makeDisplayValue(value, Health.health_red.name(), "?");
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
