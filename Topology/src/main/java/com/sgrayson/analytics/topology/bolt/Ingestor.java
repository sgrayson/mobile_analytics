package com.sgrayson.analytics.topology.bolt;

import java.util.Map;

import com.sgrayson.analytics.model.Event;
import com.sgrayson.analytics.model.EventFinish;
import com.sgrayson.analytics.util.Factory;
import com.typesafe.config.Config;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Ingestor extends BaseRichBolt {
	
	private static final long serialVersionUID = -7304291848274962553L;
	private OutputCollector collector;
	private long pastTimeCutoffMs, futureTimeCutoffMs;
	
	public Ingestor(Config config) {
		pastTimeCutoffMs 	= config.getMilliseconds("past.cutoff.time");
		futureTimeCutoffMs	= config.getMilliseconds("future.cutoff.time");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			Event event = Factory.makeEvent(tuple.getString(0));
			if (event != null && event.validate(pastTimeCutoffMs, futureTimeCutoffMs)) {
				collector.emit("persistor", new Values(event.toJson().toString()));
				if (event instanceof EventFinish) {
					String appName = event.getAppName();
					String userId  = event.getUserId();
					String eventDate = event.getEventDate();
					collector.emit("retriever", new Values(appName, userId, eventDate));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			collector.ack(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("persistor", new Fields("event"));
		declarer.declareStream("retriever", new Fields("appName", "userId", "eventDate"));
	}
	
}