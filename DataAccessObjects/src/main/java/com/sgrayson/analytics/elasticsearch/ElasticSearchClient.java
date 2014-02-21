package com.sgrayson.analytics.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticSearchClient {
	
	private Client client;
	private int batchCounter;
	private int batchValue;
	private BulkRequestBuilder bulk;
	
	public ElasticSearchClient(String hosts, int batchValue, String clusterName) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        TransportClient localClient = new TransportClient(settings);
        for (String host : hosts.split(",")) {
        	localClient.addTransportAddress(new InetSocketTransportAddress(host.split(":")[0], 9300));
        }
        client = localClient;
        this.batchValue = batchValue;
        batchCounter = 0;
        bulk = client.prepareBulk();
    }
	
	public void index(String index, String type, String id, String document, String routingKey) {
        bulk.add(client.prepareIndex(index, type, id).setSource(document).setRouting(routingKey));
        batchCounter++;
        if (batchCounter >= batchValue) {
        	bulk.execute();
            batchCounter = 0;
            bulk = client.prepareBulk();
        }
    }
}