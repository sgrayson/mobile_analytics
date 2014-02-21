package com.sgrayson.analytics.model;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;

public class User {
	
	private String appName;
	private String userId;
	private Map<String,Document> documents;
	
	public User(String appName, String userId) {
		this.appName = appName;
		this.userId = userId;
		documents = new HashMap<String,Document>();
	}
	
	public Map<String, Document> getDocuments() {
		return documents;
	}
	
	public void put(String activityDate, String name, String value) {
		Document doc = documents.get(activityDate);
		if (doc == null) {
			doc = new Document(appName, userId, activityDate);
			documents.put(activityDate, doc);
		}
		doc.put(name, value);
	}
	
	public class Document {
		
		private String appName;
		private String userId;
		private String country;
		private String osVersion;
		private String appVersion;
		private String deviceInfo;
		private String activityDate;
		private String creationDate;
		private int totalSessions;
		private int duration;
		private int numCrashes;
		private List<String> checkpoints;
		
		public Document(String appName, String userId, String activityDate) {
			this.activityDate = activityDate;
		}
		
		public String getActivityDate() {
			return activityDate;
		}
		
		public JSONObject toJson() throws Exception {
			JSONObject returnVal = new JSONObject();
			returnVal.put("appName", appName);
			returnVal.put("userId", userId);
			returnVal.put("activityDate", activityDate);
			returnVal.put("creationDate", creationDate);
			returnVal.put("country", country);
			returnVal.put("osVersion", osVersion);
			returnVal.put("appVersion", appVersion);
			returnVal.put("deviceInfo", deviceInfo);
			returnVal.put("totalSession", totalSessions);
			returnVal.put("duration", duration);
			returnVal.put("numCrashes", numCrashes);
			returnVal.put("newUser", creationDate.equals(activityDate));
			returnVal.put("checkpoints", new JSONArray(checkpoints));
			return returnVal;
		}
		
		public void put(String name, String value) {
			if (name.equals("country")) {
				country = value;
			} else if (name.equals("osVersion")) {
				osVersion = value;
			} else if (name.equals("appVersion")) {
				appVersion = value;
			} else if (name.equals("deviceInfo")) {
				deviceInfo = value;
			} else if (name.equals("duration")) {
				totalSessions++;
				duration += Integer.parseInt(value);
			} else if (name.equals("crash")) {
				numCrashes++;
			} else if (name.equals("checkpoint")) {
				checkpoints.add(value);
			} else if (name.equals("y_activityDates") && creationDate == null) {
				creationDate = value;
			}
		}
	}
}