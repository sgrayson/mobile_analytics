package com.sgrayson.analytics.topology.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import com.sgrayson.analytics.cassandra.CassandraClient;
import com.sgrayson.analytics.model.User;
import com.sgrayson.analytics.model.User.Document;
import com.typesafe.config.Config;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.commons.lang.StringUtils;

public class Retriever extends BaseRichBolt {

	private static final long serialVersionUID = -2705486396404589626L;
	private OutputCollector collector;
	private String keyspaceName, consistency, hosts;
	private int pending;
	private int batchValue;
	private CassandraClient cass;
	private Map<String, List<String>> rowsMap;
	
	public Retriever(Config config) {
		keyspaceName = config.getString("cassandra.keyspace");
		consistency  = config.getString("cassandra.consistency");
		hosts  		 = StringUtils.join(config.getStringList("cassandra.hosts.list"), ",");
		batchValue 	 = config.getInt("batch.limit");
		pending 	 = 0;
		rowsMap		 = new HashMap<String, List<String>>();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			cass = new CassandraClient("read-cluster", keyspaceName, hosts, consistency, -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			String appName 	 = tuple.getStringByField("appName");
			String userId    = tuple.getStringByField("userId");
			String eventDate = tuple.getStringByField("eventDate");
			String rowKey = appName + "_" + userId;
			List<String> userList = rowsMap.get(rowKey);
			if (userList == null) {
				userList = new ArrayList<String>();
				rowsMap.put(eventDate, userList);
			}
			userList.add(rowKey);
			pending++;
			if (pending >= batchValue) {
				handleBatch();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			collector.ack(tuple);
		}
	}
	
	private void handleBatch() throws Exception {
		Map<String,User> documents = cass.getRowData("dailyevents", rowsMap);
		for (String key : documents.keySet()) {
			User appUser = documents.get(key);
			Map<String,Document> docMap = appUser.getDocuments();
			for (String date : docMap.keySet()) {
				Document doc = docMap.get(date);
				String index = doc.getActivityDate();
				collector.emit("indexer", new Values(index, key, doc.toJson().toString()));
			}
		}
		pending = 0;
		rowsMap.clear();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("indexer", new Fields("index", "id", "document"));
	}
	
	
	
	
}