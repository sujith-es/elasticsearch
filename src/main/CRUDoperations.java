package main;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

public class CRUDoperations {

	public static void main(String[] args) throws InterruptedException {

		Settings settings = Settings.settingsBuilder()				
				.put(ElasticConstants.CLUSTER_NAME, "mycluster1").build();

		Client client = TransportClient.builder().settings(settings).build();

		IndicesOperations io = new IndicesOperations(client);

		final String indexName = "indextest";
		if (io.checkIndexExists(indexName)) {
			io.deleteIndex(indexName);
			io.createIndex(indexName);
			Thread.sleep(1000);
			io.closeIndex(indexName);
			io.openIndex(indexName);
			io.deleteIndex(indexName);
		}

	}

}
