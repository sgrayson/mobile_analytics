package com.sgrayson.analytics.cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Iterator;

import com.sgrayson.analytics.model.User;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class CassandraClient {
	
	private Mutator<String> mutator;
	private Keyspace keyspace;
	int pendingMutations, pendingMutationsLimit;
	
	public CassandraClient(String clusterName, String keyspaceName, String hosts, String consistency, int batchValue) {
		CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(hosts);
		Cluster cluster = HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator);

		ConfigurableConsistencyLevel levels = new ConfigurableConsistencyLevel();
		levels.setDefaultReadConsistencyLevel(HConsistencyLevel.valueOf(consistency.toUpperCase()));
		levels.setDefaultWriteConsistencyLevel(HConsistencyLevel.valueOf(consistency.toUpperCase()));

		keyspace = HFactory.createKeyspace(keyspaceName, cluster, levels);
		
		pendingMutationsLimit = batchValue;
		pendingMutations = 0;
		
		mutator = HFactory.createMutator(keyspace, StringSerializer.get());
	}
	
	public void insertColumns(String key, String columnFamily, Map<List<String>, String> columnValues) {
		for (List<String> columns : columnValues.keySet()) {
			String columnValue = columnValues.get(columns);
			Composite columnKey = new Composite();
			for (String column : columns) {
				columnKey.addComponent(column, StringSerializer.get());
			}
			mutator.addInsertion(key, columnFamily, 
					HFactory.createColumn(columnKey, columnValue, CompositeSerializer.get(), StringSerializer.get()));
			pendingMutations++;
		}
		if (pendingMutations >= pendingMutationsLimit) {
			handleBatch();
		}
	}
	
	public Map<String, User> getRowData(String columnFamily, Map<String, List<String>> rows) throws Exception {
		Map<String, User> documents = new HashMap<String, User>();
		
		for (String key : rows.keySet()) {
			MultigetSliceQuery<String, Composite, String> query = 
					HFactory.createMultigetSliceQuery(keyspace, 
							StringSerializer.get(), CompositeSerializer.get(), StringSerializer.get());
			
			Composite start = new Composite();
			start.addComponent(0, key, Composite.ComponentEquality.EQUAL);
			Composite end = new Composite();
			end.addComponent(0, "z", Composite.ComponentEquality.GREATER_THAN_EQUAL);
			
			query.setColumnFamily(columnFamily);
			query.setKeys(rows.get(key).toArray(new String[] {}));
			query.setRange(start, end, false, 10000);
			
			QueryResult<Rows<String,Composite,String>> results = query.execute();
			Iterator<Row<String,Composite,String>> iterator = results.get().iterator();
			
			while (iterator.hasNext()) {
				Row<String,Composite,String> row = iterator.next();
				ColumnSlice<Composite,String> slice = row.getColumnSlice();
				
				String rowKey = row.getKey();
				User user = documents.get(rowKey);
				if (user == null) {
					user = new User(rowKey.split("_")[0], rowKey.split("_")[1]);
					documents.put(rowKey, user);
				}
				
				for (HColumn<Composite,String> column : slice.getColumns()) {
					List<Component<?>> components = column.getName().getComponents();
					String activityDate = (String) components.get(0).getValue();
					String columnName = (String) components.get(1).getValue();
					String columnValue = column.getValue();
					
					user.put(activityDate, columnName, columnValue);
				}
			}
		}
		return documents;
	}
	
	private void handleBatch() {
		mutator.execute();
		mutator = HFactory.createMutator(keyspace, StringSerializer.get());
		pendingMutations = 0;
	}
}








