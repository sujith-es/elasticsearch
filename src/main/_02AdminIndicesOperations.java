package main;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;

public class _02AdminIndicesOperations {

	private final Client client;

	public _02AdminIndicesOperations(Client client) {
		this.client = client;
	}

	public boolean checkIndexExists(final String name) {

		IndicesExistsResponse response = client.admin().indices()
				.prepareExists(name).execute().actionGet();

		return response.isExists();
	}

	public void createIndex(final String name) {
		client.admin().indices().prepareCreate(name).execute().actionGet();
	}

	public void deleteIndex(final String name) {
		client.admin().indices().prepareDelete(name).execute().actionGet();
	}

	public void closeIndex(final String name) {
		client.admin().indices().prepareClose(name).execute().actionGet();
	}

	public void openIndex(final String name) {
		client.admin().indices().prepareOpen(name).execute().actionGet();
	}
	
	

}
