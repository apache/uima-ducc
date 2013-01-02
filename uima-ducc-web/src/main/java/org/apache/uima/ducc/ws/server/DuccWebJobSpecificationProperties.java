/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.uima.ducc.ws.server;

import javax.servlet.http.HttpServletRequest;

import org.apache.uima.ducc.cli.DuccUiConstants;
import org.apache.uima.ducc.transport.event.cli.JobSpecificationProperties;


@SuppressWarnings("serial")
public class DuccWebJobSpecificationProperties extends JobSpecificationProperties {
	
	private static int inputSize = 30;
	
	private static String help(String helptext) {
		return "<img title=\""+helptext+"\" src=\"images/qmark.png\"/>";
	}
	
	private static String genInput(String key, String initialValue) {
		if(initialValue == null) {
			return "<input type=\"text\" size=\""+inputSize+"\" id=\""+key+"\" />";
		}
		else {
			return "<input type=\"text\" size=\""+inputSize+"\" id=\""+key+"\" value=\""+initialValue+"\"/>";
		}
	}
	
	private static String genSelect(String key, String initialValue, String[] possibleValues) {
		StringBuffer sb = new StringBuffer();
		sb.append("<select id=\""+key+"\">");
		for (int i=0;i<possibleValues.length;i++) { 
			String selected = "";
			if(initialValue != null) {
				if(initialValue.equals(possibleValues[i])) {
					selected = " selected=\"selected\"";
				}
			}
			sb.append("<option"+selected+">"+possibleValues[i]+"</option>");
		}
		sb.append("</select>");
		return sb.toString();
	}
	
	private static String entry(String description, String key, String label, String initialValue, String[] possibleValues, String color, String hover) {
		StringBuffer sb = new StringBuffer();
		sb.append("<tr>");
		sb.append("<td>");
		sb.append(help(description));
		sb.append("<td>");
		if(possibleValues == null) {
			sb.append(genInput(key,initialValue));
		}
		else {
			sb.append(genSelect(key,initialValue,possibleValues));
		}
		sb.append("<td align=\"left\">");
		sb.append("<span title=\""+hover+"\" style=\"color: "+color+"\">*</span>");
		sb.append("<span title=\""+hover+"\" >"+label+"</span>");
		return sb.toString();
	}

	private static String entry(String description, String key, String label, String initialValue, String[] possibleValues) {
		return entry(description, key, label, initialValue, possibleValues, "white", "");
	}
	
	private static String descriptionWithExample(String description, String exampleText) {
		return description+" Example: "+exampleText;
	}
	
	private static String presetValue(String cookieValue, String defaultValue) {
		String retVal = null;
		if(cookieValue != null) {
			retVal = cookieValue;
		}
		else if(defaultValue != null) {
			retVal = defaultValue;
		}
		return retVal;
	}
	
	public static String getHtmlForm(HttpServletRequest request, DuccWebSchedulerClasses schedulerClasses) 
	{
		StringBuffer sb = new StringBuffer();
		sb.append("<fieldset>");
		sb.append("<legend>");
		sb.append("Job");
		sb.append("<table>");
		
		sb.append(entry(descriptionWithExample(DuccUiConstants.desc_description,DuccUiConstants.exmp_description),
				key_description,
				DuccUiConstants.labl_description,
				null,
				null
				));
		
		sb.append(entry(descriptionWithExample(DuccUiConstants.desc_jvm,DuccUiConstants.exmp_jvm),
				key_jvm,
				DuccUiConstants.labl_jvm,
				presetValue(DuccWebUtil.getCookieOrNull(request,DuccWebUtil.getCookieKey(DuccUiConstants.name_jvm)),DuccUiConstants.dval_jvm),
				null
				));
		
		sb.append(entry(descriptionWithExample(DuccUiConstants.desc_scheduling_class,DuccUiConstants.exmp_scheduling_class),
				key_scheduling_class,
				DuccUiConstants.labl_scheduling_class,
				presetValue(DuccWebUtil.getCookieOrNull(request,DuccWebUtil.getCookieKey(DuccUiConstants.name_scheduling_class)),DuccUiConstants.dval_scheduling_class),
				schedulerClasses.getJobClasses()
				));
		
		sb.append(entry(descriptionWithExample(DuccUiConstants.desc_log_directory,DuccUiConstants.exmp_log_directory),
				key_log_directory,
				DuccUiConstants.labl_log_directory,
				presetValue(DuccWebUtil.getCookieOrNull(request,DuccWebUtil.getCookieKey(DuccUiConstants.name_log_directory)),DuccUiConstants.dval_log_directory),
				null
				));
		
		sb.append(entry(descriptionWithExample(DuccUiConstants.desc_working_directory,DuccUiConstants.exmp_working_directory),
				key_working_directory,
				DuccUiConstants.labl_working_directory,
				presetValue(DuccWebUtil.getCookieOrNull(request,DuccWebUtil.getCookieKey(DuccUiConstants.name_working_directory)),DuccUiConstants.dval_working_directory),
				null
				));
		
		sb.append("</table>");
		sb.append("</legend>");
		sb.append("</fieldset>");
		
		sb.append("<fieldset>");
		sb.append("<legend>");
		sb.append("Driver");
		sb.append("<table>");
		
		sb.append(entry(descriptionWithExample(DuccUiConstants.desc_driver_jvm_args,DuccUiConstants.exmp_driver_jvm_args),
				key_driver_jvm_args,
				DuccUiConstants.labl_driver_jvm_args,
				null,
				null
		));
		sb.append(entry(DuccUiConstants.desc_driver_classpath,
				key_driver_classpath,
				DuccUiConstants.labl_driver_classpath,
				null,
				null
		));
		sb.append(entry(DuccUiConstants.desc_driver_environment,
				key_driver_environment,
				DuccUiConstants.labl_driver_environment,
				null,
				null
		));
		sb.append(entry(DuccUiConstants.desc_driver_memory_size,
				key_driver_memory_size,
				DuccUiConstants.labl_driver_memory_size,
				null,
				null
		));
		sb.append(entry(descriptionWithExample(DuccUiConstants.desc_driver_descriptor_CR,DuccUiConstants.exmp_driver_descriptor_CR),
				key_driver_descriptor_CR,
				DuccUiConstants.labl_driver_descriptor_CR,
				null,
				null,
				"red",
				"CR is required."
		));
		sb.append(entry(descriptionWithExample(DuccUiConstants.desc_driver_descriptor_CR_overrides,DuccUiConstants.exmp_driver_descriptor_CR_overrides),
				key_driver_descriptor_CR_overrides,
				DuccUiConstants.labl_driver_descriptor_CR_overrides,
				null,
				null
		));
		
		sb.append("</table>");
		sb.append("</legend>");
		sb.append("</fieldset>");
		
		sb.append("<fieldset>");
		sb.append("<legend>");
		sb.append("Process");
		sb.append("<table>");
		
		sb.append(entry(descriptionWithExample(DuccUiConstants.desc_process_jvm_args,DuccUiConstants.exmp_process_jvm_args),
				key_process_jvm_args,
				DuccUiConstants.labl_process_jvm_args,
				null,
				null
		));
		sb.append(entry(DuccUiConstants.desc_process_classpath,
				key_process_classpath,
				DuccUiConstants.labl_process_classpath,
				null,
				null
		));
		sb.append(entry(DuccUiConstants.desc_process_environment,
				key_process_environment,
				DuccUiConstants.labl_process_environment,
				null,
				null
		));
		sb.append(entry(DuccUiConstants.desc_process_memory_size,
				key_process_memory_size,
				DuccUiConstants.labl_process_memory_size,
				null,
				null
		));
		sb.append(entry(DuccUiConstants.desc_process_descriptor_CM,
				key_process_descriptor_CM,
				DuccUiConstants.labl_process_descriptor_CM,
				null,
				null,
				"orange",
				"At least one of { CM, AE, CC } is required."
		));
		sb.append(entry(DuccUiConstants.desc_process_descriptor_AE,
				key_process_descriptor_AE,
				DuccUiConstants.labl_process_descriptor_AE,
				null,
				null,
				"orange",
				"At least one of { CM, AE, CC } is required."
		));
		sb.append(entry(DuccUiConstants.desc_process_descriptor_CC,
				key_process_descriptor_CC,
				DuccUiConstants.labl_process_descriptor_CC,
				null,
				null,
				"orange",
				"At least one of { CM, AE, CC } is required."
		));
		sb.append(entry(DuccUiConstants.desc_process_deployments_max,
				key_process_deployments_max,
				DuccUiConstants.labl_process_deployments_max,
				presetValue(DuccWebUtil.getCookieOrNull(request,DuccWebUtil.getCookieKey(DuccUiConstants.name_process_deployments_max)),DuccUiConstants.dval_process_deployments_max),
				null
		));
		/*
		sb.append(entry(DuccUiConstants.desc_process_deployments_min,
				key_process_deployments_min,
				DuccUiConstants.labl_process_deployments_min,
				presetValue(DuccWebUtil.getCookieOrNull(request,DuccWebUtil.getCookieKey(DuccUiConstants.name_process_deployments_min)),DuccUiConstants.dval_process_deployments_min),
				null
		));
		*/
		sb.append(entry(DuccUiConstants.desc_process_thread_count,
				key_process_thread_count,
				DuccUiConstants.labl_process_thread_count,
				presetValue(DuccWebUtil.getCookieOrNull(request,DuccWebUtil.getCookieKey(DuccUiConstants.name_process_thread_count)),DuccUiConstants.dval_process_thread_count),
				null
		));
		sb.append(entry(DuccUiConstants.desc_process_get_meta_time_max,
				key_process_get_meta_time_max,
				DuccUiConstants.labl_process_get_meta_time_max,
				presetValue(DuccWebUtil.getCookieOrNull(request,DuccWebUtil.getCookieKey(DuccUiConstants.name_process_get_meta_time_max)),DuccUiConstants.dval_process_get_meta_time_max),
				null
		));
		sb.append(entry(DuccUiConstants.desc_process_per_item_time_max,
				key_process_per_item_time_max,
				DuccUiConstants.labl_process_per_item_time_max,
				presetValue(DuccWebUtil.getCookieOrNull(request,DuccWebUtil.getCookieKey(DuccUiConstants.name_process_per_item_time_max)),DuccUiConstants.dval_process_per_item_time_max),
				null
		));
		
		sb.append("</table>");
		sb.append("</legend>");
		sb.append("</fieldset>");
		
		return sb.toString();
	}
}
