package com.sgrayson.analytics.model;

import org.json.JSONObject;

public class EventStart extends Event {
	
	private String country;
	private String osVersion;
	private String appVersion;
	private String deviceInfo;
	
	public EventStart(JSONObject event) throws Exception {
		super(event.getString("appName"), event.getString("udid"), event.getString("sid"), event.getDouble("eventTime"));
		country = event.getString("country");
		osVersion = event.getString("osVersion");
		appVersion = event.getString("appVersion");
		deviceInfo = event.getString("deviceInfo");
	}
	
	public String getCountry() {
		return country;
	}
	
	public String getOsVersion() {
		return osVersion;
	}
	
	public String getAppVersion() {
		return appVersion;
	}
	
	public String getDeviceInfo() {
		return deviceInfo;
	}
	
	@Override
	public JSONObject toJson() throws Exception {
		JSONObject returnVal = super.toJson();
		returnVal.put("country", getCountry());
		returnVal.put("osVersion", getOsVersion());
		returnVal.put("appVersion", getAppVersion());
		returnVal.put("deviceInfo", getDeviceInfo());
		return returnVal;
	}
}