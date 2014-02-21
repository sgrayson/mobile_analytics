package com.sgrayson.analytics.topology.bolt;

import java.util.Map;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.List;
import java.text.SimpleDateFormat;

import com.sgrayson.analytics.cassandra.CassandraClient;
import com.sgrayson.analytics.util.Factory;
import com.typesafe.config.Config;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

public class Persistor extends BaseRichBolt {

	private static final long serialVersionUID = 769397823077908369L;
	private OutputCollector collector;
	private String keyspaceName, consistency, hosts;
	private int batchValue;
	private CassandraClient cass;
	private SimpleDateFormat sdf;
	private GregorianCalendar cal;
	
	public Persistor(Config config) {
		keyspaceName = config.getString("cassandra.keyspace");
		consistency  = config.getString("cassandra.consistency");
		hosts  		 = StringUtils.join(config.getStringList("cassandra.hosts.list"), ",");
		batchValue 	 = config.getInt("cassandra.batch.limit");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			cass = new CassandraClient("write-cluster", keyspaceName, hosts, consistency, batchValue);
			sdf = new SimpleDateFormat("yyyyMMdd");
			cal = new GregorianCalendar();
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			JSONObject event = new JSONObject(tuple.getStringByField("event"));
			insertEvent(event);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			collector.ack(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// this bolt does not emit any streams
	}
	
	private void insertEvent(JSONObject event) {
		try {
			String appName = (String) event.remove("appName");
			String userId  = (String) event.remove("userId");
			String key = appName + "_" + userId;
			Map<List<String>, String> columnMap = Factory.makeCassandraMap(event);
			cass.insertColumns(key, "dailyevents", columnMap);		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}