package com.sgrayson.analytics.topology.bolt;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.sgrayson.analytics.elasticsearch.ElasticSearchClient;
import com.typesafe.config.Config;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class Indexer extends BaseRichBolt {

	private static final long serialVersionUID = -5716756446079941581L;
	private OutputCollector collector;
	private int batchValue;
	private String clusterName;
	private String hosts;
	private ElasticSearchClient esclient;
	
	public Indexer(Config config) {
		hosts  		= StringUtils.join(config.getStringList("elasticsearch.hosts.list"), ",");
		clusterName = config.getString("elasticsearch.cluster.name");
		batchValue 	= config.getInt("elasticsearch.batch.limit");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			esclient = new ElasticSearchClient(hosts, batchValue, clusterName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			String index = tuple.getStringByField("index");
			String id = tuple.getStringByField("id");
			String routingKey = id.split("_")[0];
			String document = tuple.getStringByField("document");
			
			esclient.index(index, "user", id, document, routingKey);
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
}