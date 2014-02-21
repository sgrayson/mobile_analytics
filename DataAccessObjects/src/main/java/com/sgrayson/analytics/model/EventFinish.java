package com.sgrayson.analytics.model;

import org.json.JSONObject;

public class EventFinish extends Event {
	
	private int duration;
	
	public EventFinish(JSONObject event) throws Exception {
		super(event.getString("appName"), event.getString("udid"), event.getString("sid"), event.getDouble("eventTime"));
		duration = (int) event.getDouble("duration");
	}
	
	public int getDuration() {
		return duration;
	}
	
	@Override
	public JSONObject toJson() throws Exception {
		JSONObject returnVal = super.toJson();
		returnVal.put("duration", getDuration());
		return returnVal;
	}
}