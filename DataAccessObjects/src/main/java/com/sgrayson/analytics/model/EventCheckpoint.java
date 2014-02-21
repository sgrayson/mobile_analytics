package com.sgrayson.analytics.model;

import org.json.JSONObject;

public class EventCheckpoint extends Event {
	
	private String checkpoint;
	
	public EventCheckpoint(JSONObject event) throws Exception {
		super(event.getString("appName"), event.getString("udid"), event.getString("sid"), event.getDouble("eventTime"));
		checkpoint = event.getString("checkpoint");
		if (checkpoint.length() > 100) {
			checkpoint = checkpoint.substring(0, 100);
		}
	}
	
	public String getCheckpoint() {
		return checkpoint;
	}
	
	@Override
	public JSONObject toJson() throws Exception {
		JSONObject returnVal = super.toJson();
		returnVal.put("checkpoint", getCheckpoint());
		return returnVal;
	}
}