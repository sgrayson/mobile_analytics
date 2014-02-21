package com.sgrayson.analytics.topology;

import java.util.List;
import java.io.File;

import com.sgrayson.analytics.topology.bolt.Indexer;
import com.sgrayson.analytics.topology.bolt.Ingestor;
import com.sgrayson.analytics.topology.bolt.Persistor;
import com.sgrayson.analytics.topology.bolt.Retriever;
import com.typesafe.config.ConfigFactory;

import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

public class Main {
	
	private static KafkaSpout prepareSpout(com.typesafe.config.Config p) {
		int numPartitions		= p.getInt("spout.kafka.num.partitions.per.node");
		String topic			= p.getString("spout.kafka.topic");
		List<HostPort> hosts	= KafkaConfig.convertHosts(p.getStringList("spout.kafka.brokers.list"));

		String zookeeperRoot	= p.getString("spout.zookeeper.path");
		String zookeeperId		= p.getString("spout.zookeeper.consumer.id");

		long offsetTime		= p.getLong("spout.kafka.start.time");
		int fetchSize		= p.getInt("spout.kafka.fetch.size");

		SpoutConfig spoutConf	= new SpoutConfig(new StaticHosts(hosts, numPartitions), topic, zookeeperRoot, zookeeperId);
		spoutConf.scheme		= new StringScheme();
		if (offsetTime != -3)
			spoutConf.forceStartOffsetTime(offsetTime);
		spoutConf.fetchSizeBytes = fetchSize;

		return new KafkaSpout(spoutConf);
	}
	
	private static backtype.storm.Config prepareTopologyConfig(com.typesafe.config.Config config) {
		boolean debug = config.getBoolean("topo.config.debug");
		boolean fallBack = config.getBoolean("topo.config.java.serialization");
		int numAckers = config.getInt("topo.config.num.ackers");
		int numWorkers = config.getInt("topo.config.num.workers");
		int pending = config.getInt("topo.config.max.pending");
		int timeout = config.getInt("topo.config.max.timeout");

		backtype.storm.Config stormConfig = new backtype.storm.Config();

		stormConfig.setDebug(debug);
		stormConfig.setFallBackOnJavaSerialization(fallBack);
		stormConfig.setNumAckers(numAckers);
		stormConfig.setNumWorkers(numWorkers);
		stormConfig.setMaxSpoutPending(pending);
		stormConfig.setMessageTimeoutSecs(timeout);

		return stormConfig;
	}
	
	private static TopologyBuilder prepareTopologyBuilder(com.typesafe.config.Config config) {
		int spoutPar		= config.getInt("topo.builder.spout.parallelism");
		int ingestorPar 	= config.getInt("topo.builder.ingestor.parallelism");
		int persistorPar	= config.getInt("topo.builder.persistor.parallelism");
		int retreiverPar 	= config.getInt("topo.builder.retriever.parallelism");
		int indexerPar 		= config.getInt("topo.builder.indexer.parallelism");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", prepareSpout(config), spoutPar);
		builder.setBolt("ingestor", new Ingestor(config.getConfig("bolt.ingestor")), ingestorPar)
			.shuffleGrouping("spout");
		builder.setBolt("persistor", new Persistor(config.getConfig("bolt.persistor")), persistorPar)
			.shuffleGrouping("ingestor", "persistor");
		builder.setBolt("retriever", new Retriever(config.getConfig("bolt.retriever")), retreiverPar)
			.shuffleGrouping("ingestor", "retriever");
		builder.setBolt("indexer", new Indexer(config.getConfig("bolt.indexer")), indexerPar)
			.shuffleGrouping("retriever", "indexer");

		return builder;
	}
	
    public static void main( String[] args ) {
    	com.typesafe.config.Config topologyConfig;
		if (args != null && args.length == 2) {
			topologyConfig = ConfigFactory.parseFile(new File(args[1])).resolve();
		} else {
			System.out.println("Usage: ./storm jar topologyFile.jar driverClass topologyName confFilePath");
			return;
		}
		
		try {
			backtype.storm.Config stormConfig = prepareTopologyConfig(topologyConfig);
			TopologyBuilder topologyBuilder = prepareTopologyBuilder(topologyConfig);
			StormSubmitter.submitTopology(args[0], stormConfig, topologyBuilder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
