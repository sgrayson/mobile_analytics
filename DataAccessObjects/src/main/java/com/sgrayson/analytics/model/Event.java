package com.sgrayson.analytics.model;

import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

import org.json.JSONObject;

public class Event {
	
	private String appName;
	private String userId;
	private String sessionId;
	private GregorianCalendar eventTime;
	private SimpleDateFormat sdf;
	
	public Event(String appName, String userId, String sessionId, double eventTime) {
		this.appName = appName;
		this.userId = userId;
		this.sessionId = sessionId;
		sdf = new SimpleDateFormat("yyyyMMdd");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		this.eventTime = parseEventTime(eventTime);
	}
	
	public String getAppName() {
		return appName;
	}
	
	public String getUserId() {
		return userId;
	}
	
	public String getSessionId() {
		return sessionId;
	}
	
	public long getEventTime() {
		return eventTime.getTimeInMillis();
	}
	
	public String getEventDate() {
		return sdf.format(eventTime.getTime());
	}
	
	public String getEventDate(String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(eventTime.getTime());
	}
	
	private static GregorianCalendar parseEventTime(double d) {
        GregorianCalendar eventTime = new GregorianCalendar();
        eventTime.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (d < (System.currentTimeMillis() / 100)) {
            d = d * 1000;
        }
        eventTime.setTimeInMillis((long) d);
        return eventTime;
    }
	
	public JSONObject toJson() throws Exception {
		JSONObject returnVal = new JSONObject();
		returnVal.put("appName", getAppName());
		returnVal.put("userId", getUserId());
		returnVal.put("eventDate", getEventDate());
		returnVal.put("sessionId", getSessionId());
		return returnVal;
	}
	
	public boolean validate(long pastTimeCutoffMs, long futureTimeCutoffMs) {
		long presentTime = System.currentTimeMillis();
		long eventTime = this.eventTime.getTimeInMillis();
		return ((eventTime >= (presentTime - pastTimeCutoffMs)) && 
			(eventTime <= (presentTime + futureTimeCutoffMs)));
	}
}