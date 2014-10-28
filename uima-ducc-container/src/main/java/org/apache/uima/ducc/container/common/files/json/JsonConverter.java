package org.apache.uima.ducc.container.common.files.json;

import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JsonConverter {

	private static Gson gson = new Gson();
	
	private static String linend = "\n";
	
	public static String workItemStateToJson(IJsonWorkItemState jsonObject) {
		String json = gson.toJson(jsonObject)+linend;
		return json;
	}
	
	public static IJsonWorkItemState workItemStateFromJson(String jsonString) {
		Type typeOfMap = new TypeToken<JsonWorkItemState>() { }.getType();
		IJsonWorkItemState retVal = gson.fromJson(jsonString, typeOfMap);
		return retVal;
	}
	
	public static String workItemStateMapToJson(ConcurrentHashMap<String,JsonWorkItemState> jsonObject) {
		String json = gson.toJson(jsonObject);
		return json;
	}
	
	public static ConcurrentHashMap<String,JsonWorkItemState> workItemStateMapFromJson(String jsonString) {
		Type typeOfMap = new TypeToken<ConcurrentHashMap<String,JsonWorkItemState>>() { }.getType();
		ConcurrentHashMap<String,JsonWorkItemState> retVal = gson.fromJson(jsonString, typeOfMap);
		return retVal;
	}
}
