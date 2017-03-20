package org.apache.uima.ducc.orchestrator.system.events.log;

/*
 * Use the methods of this class to record system events to the system-event.log file.
 * This class is located in the Orchestrator since it alone is given responsibility for this task.
 */
import java.util.Properties;

import org.apache.uima.ducc.transport.event.CancelJobDuccEvent;
import org.apache.uima.ducc.transport.event.CancelJobReplyDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationReplyDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceDuccEvent;
import org.apache.uima.ducc.transport.event.CancelServiceReplyDuccEvent;
import org.apache.uima.ducc.transport.event.ServiceReplyEvent;
import org.apache.uima.ducc.transport.event.ServiceRequestEvent;
import org.apache.uima.ducc.transport.event.SubmitJobDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitJobReplyDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationReplyDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobReplyProperties;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationReplyProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceReplyProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.IDuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService;
import org.apache.uima.ducc.transport.event.common.IRationale;

public class SystemEventsLogger {

	private static DuccLogger duccLogger = getEventLogger(SystemEventsLogger.class.getName());
	
	static public DuccLogger makeLogger(String claz, String componentId) {
        return DuccLogger.getLogger(claz, componentId);
	}
	
	static public DuccLogger getEventLogger(String claz)
    {
        return makeLogger("org.apache.uima.ducc.system.events.log", "event");
    }
	
	/*
	 * log a warning - nominally daemon start or stop
	 */
	public static void warn(String daemon, String state, String text) {
		String user = System.getProperty("user.name");
		String type = state;
		Object[] event = { };
		duccLogger.event_warn(daemon, user, type, event);
	}
	
	/*
	 * convenience method to get a property from a properties file if it exists,
	 * else return the default value specified
	 */
	private static String getProperty(Properties properties, String key, String defaultValue) {
		String retVal = defaultValue;
		if(properties != null) {
			if(key != null) {
				if(properties.containsKey(key)) {
					retVal = properties.getProperty(key);
				}
			}
		}
		return retVal;
	}
	
	/*
	 * convenience method to get a property from a properties file if it exists,
	 * else return the default value of "N/A"
	 */
	private static String getProperty(Properties properties, String key) {
		return getProperty(properties, key, "N/A");
	}
	
	/*
	 * log a job submit request
	 */
	public static void info(String daemon, SubmitJobDuccEvent request, SubmitJobReplyDuccEvent response) {
		Properties qprops = request.getProperties();
		String user = getProperty(qprops, JobRequestProperties.key_user);
		String type = request.getEventType().name();
		String id = getProperty(qprops, JobRequestProperties.key_id);
		String sclass = getProperty(qprops, JobRequestProperties.key_scheduling_class);
		String size = getProperty(qprops, JobRequestProperties.key_process_memory_size);
		Properties rprops = response.getProperties();
		String message = getProperty(rprops, JobReplyProperties.key_message, "");
		Object[] event = { "id:"+id, "class:"+sclass, "size:"+size, message };
		duccLogger.event_info(daemon, user, type, event);
	}
	
	/*
	 * log a job cancel request
	 */
	public static void info(String daemon, CancelJobDuccEvent request, CancelJobReplyDuccEvent response) {
		Properties qprops = request.getProperties();
		String user = getProperty(qprops, JobRequestProperties.key_user);
		String type = request.getEventType().name();
		String id = getProperty(qprops, JobRequestProperties.key_id);
		Properties rprops = response.getProperties();
		String message = getProperty(rprops, JobReplyProperties.key_message);
		Object[] event = { "id:"+id, message };
		duccLogger.event_info(daemon, user, type, event);
	}
	
	/*
	 * log a job state change - nominally Completed only is logged
	 */
	public static void info(String daemon, String state, IDuccWorkJob job) {
		String user = job.getStandardInfo().getUser();
		String type = state;
		String id = job.getId();
		String reason = job.isCompleted() ? job.getCompletionType().name() : "";
		String rationale = "";
		IRationale completionRationale = job.getCompletionRationale();
		if(completionRationale != null) {
			rationale = completionRationale.getText();
		}
		Object[] event = { "id:"+id,reason,rationale };
		duccLogger.event_info(daemon, user, type, event);
	}
	
	/*
	 * log a reservation submit request
	 */
	public static void info(String daemon, SubmitReservationDuccEvent request, SubmitReservationReplyDuccEvent response) {
		Properties qprops = request.getProperties();
		String user = getProperty(qprops, ReservationRequestProperties.key_user);
		String type = request.getEventType().name();
		String id = getProperty(qprops, ReservationRequestProperties.key_id);
		String sclass = getProperty(qprops, ReservationRequestProperties.key_scheduling_class);
		String size = getProperty(qprops, ReservationRequestProperties.key_memory_size);
		Properties rprops = response.getProperties();
		String message = getProperty(rprops, ReservationReplyProperties.key_message, "");
		Object[] event = { "id:"+id, "class:"+sclass, "size:"+size, message };
		duccLogger.event_info(daemon, user, type, event);
	}
	
	/*
	 * log a reservation cancel request
	 */
	public static void info(String daemon, CancelReservationDuccEvent request, CancelReservationReplyDuccEvent response) {
		Properties qprops = request.getProperties();
		String user = getProperty(qprops, ReservationRequestProperties.key_user);
		String type = request.getEventType().name();
		String id = getProperty(qprops, ReservationRequestProperties.key_id);
		Properties rprops = response.getProperties();
		String message = getProperty(rprops, ReservationReplyProperties.key_message);
		Object[] event = { "id:"+id, message };
		duccLogger.event_info(daemon, user, type, event);
	}
	
	/*
	 * log a reservation state change - nominally Completed only is logged
	 */
	public static void info(String daemon, String state, IDuccWorkReservation reservation) {
		String user = reservation.getStandardInfo().getUser();
		String type = state;
		String id = reservation.getId();
		String reason = reservation.isCompleted() ? reservation.getCompletionType().name() : "";
		String rationale = "";
		IRationale completionRationale = reservation.getCompletionRationale();
		if(completionRationale != null) {
			rationale = completionRationale.getText();
		}
		Object[] event = { "id:"+id,reason,rationale };
		duccLogger.event_info(daemon, user, type, event);
	}
	
	/*
	 * log a service submit request
	 */
	public static void info(String daemon, SubmitServiceDuccEvent request, SubmitServiceReplyDuccEvent response) {
		Properties qprops = request.getProperties();
		String user = getProperty(qprops, ServiceRequestProperties.key_user);
		String type = request.getEventType().name();
		String id = getProperty(qprops, ServiceRequestProperties.key_id);
		String sclass = getProperty(qprops, ServiceRequestProperties.key_scheduling_class);
		String size = getProperty(qprops, ServiceRequestProperties.key_process_memory_size);
		Properties rprops = response.getProperties();
		String message = getProperty(rprops, ServiceReplyProperties.key_message, "");
		Object[] event = { "id:"+id, "class:"+sclass, "size:"+size, message };
		duccLogger.event_info(daemon, user, type, event);
	}
	
	/*
	 * log a service cancel request
	 */
	public static void info(String daemon, CancelServiceDuccEvent request, CancelServiceReplyDuccEvent response) {
		Properties properties = request.getProperties();
		String user = properties.getProperty(ServiceRequestProperties.key_user);
		String type = request.getEventType().name();
		String id = properties.getProperty(ServiceRequestProperties.key_id);
		Properties rprops = response.getProperties();
		String message = getProperty(rprops, ReservationReplyProperties.key_message);
		Object[] event = { "id:"+id, message };
		duccLogger.event_info(daemon, user, type, event);
	}
	
	/*
	 * log a service state change - nominally Completed only is logged
	 */
	public static void info(String daemon, String state, IDuccWorkService service) {
		String user = service.getStandardInfo().getUser();
		String type = state;
		String id = service.getId();
		String reason = "";
		//String reason = service.isCompleted() ? service.getCompletionType().name() : "";
		String rationale = "";
		//IRationale completionRationale = service.getCompletionRationale();
		//if(completionRationale != null) {
		//	rationale = completionRationale.getText();
		//}
		Object[] event = { "id:"+id,reason,rationale };
		duccLogger.event_info(daemon, user, type, event);
	}
	
	/*
	 * log a services request
	 */
	public static void info(String daemon, ServiceRequestEvent request, ServiceReplyEvent response) {
		String user = request.getUser();
		String type = request.getEventType().name();
		long id = response.getId();
		boolean rc = response.getReturnCode();
		String message = response.getMessage();
		Object[] event = { "id:"+id, "rc:"+rc, message};
		duccLogger.event_info(daemon, user, type, event);
	}
	
}
