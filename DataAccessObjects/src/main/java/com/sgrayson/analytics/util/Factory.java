package com.sgrayson.analytics.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.json.JSONObject;

import com.sgrayson.analytics.model.Event;
import com.sgrayson.analytics.model.EventCheckpoint;
import com.sgrayson.analytics.model.EventCrash;
import com.sgrayson.analytics.model.EventFinish;
import com.sgrayson.analytics.model.EventStart;

public class Factory {
	
	public static Event makeEvent(String tuple) {
		Event event = null;
		try {
			JSONObject obj = new JSONObject(tuple);
			String eventType = obj.getString("eventType");
			if (eventType.equals("SESSIONCREATION")) {
				event = new EventStart(obj);
			} else if (eventType.equals("CRASH")) {
				event = new EventCrash(obj);
			} else if (eventType.equals("CHECKPOINT")) {
				event = new EventCheckpoint(obj);
			} else if (eventType.equals("END")) {
				event = new EventFinish(obj);
			} 
		} catch (Exception e) {
			e.printStackTrace();
		}
		return event;
	}
	
	public static Map<List<String>, String> makeCassandraMap(JSONObject event) throws Exception {
		Map<List<String>, String> returnVal = new HashMap<List<String>, String>();
		String activityDate = (String) event.remove("eventDate");
		String sessionId = (String) event.remove("sessionId");
		Iterator<?> iterator = event.keys();
		while (iterator.hasNext()) {
			String columnName = (String) iterator.next();
			if (columnName.equals("duration")) {
				String value = Integer.toString((int)event.getDouble(columnName));
				returnVal.put(createList(activityDate, columnName, sessionId), value);
			} else if (columnName.equals("checkpoint") || columnName.equals("crash")) {
				String value = event.getString(columnName);
				returnVal.put(createList(activityDate, columnName, value), value);
			} else {
				returnVal.put(createList(activityDate, columnName), event.getString(columnName));
			}
		}
		returnVal.put(createList("y_activityDates", activityDate), activityDate);
		return returnVal;
	}
	
	public static List<String> createList(String... items) {
		List<String> list = new ArrayList<String>();
		for (int i = 0; i < items.length; i++) {
			list.add(items[i]);
		}
		return list;
	}
}