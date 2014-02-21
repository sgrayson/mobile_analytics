package com.sgrayson.analytics.model;

import org.json.JSONObject;

public class EventCrash extends Event {
	
	private String md5hash;
	
	public EventCrash(JSONObject event) throws Exception {
		super(event.getString("appName"), event.getString("udid"), event.getString("sid"), event.getDouble("eventTime"));
		md5hash = event.getString("md5OfCrash");
	}
	
	public String getMd5OfCrash() {
		return md5hash;
	}
	
	@Override
	public JSONObject toJson() throws Exception {
		JSONObject returnVal = super.toJson();
		returnVal.put("crash", getMd5OfCrash());
		return returnVal;
	}
	
}