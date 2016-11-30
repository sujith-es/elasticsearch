package com.CRUD;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchUtil {

	private static final Logger logger = LoggerFactory
			.getLogger(ElasticSearchUtil.class);
	private static final String CLUSTER_NAME = "mycluster1";
	private static final String ELASTIC_SERVER_HOST = "localhost";
	private static final int ELASTIC_SERVER_PORT = 9200;
	private static final int ELASTIC_INDEX_PORT = 9300;

	public static TransportClient getTransportClient(final String cluster,
			final String host, final int port) {

		try {
			String cluster_name = cluster == null ? CLUSTER_NAME : cluster;
			String host_name = host == null ? ELASTIC_SERVER_HOST : host;
			int port_address = port == 0 ? ELASTIC_SERVER_PORT : port;

			Settings settings = Settings.settingsBuilder()
					.put(ElasticConstants.CLUSTER_NAME, cluster_name).build();

			TransportClient client = TransportClient
					.builder()
					.settings(settings)
					.build()
					.addTransportAddress(
							new InetSocketTransportAddress(InetAddress
									.getByName(host_name), port_address));

			System.out
					.println("---------------------getTransportClient() TransportClient reponse:-----------------------");
			System.out.println(client.toString());

			return client;

		} catch (Exception e) {
			logger.error("getTransportClient", e);
		}

		return null;
	}

	public static Client getClient() {

		try {
			return getTransportClient(CLUSTER_NAME, ELASTIC_SERVER_HOST,
					ELASTIC_INDEX_PORT);
		} catch (Exception e) {
			logger.error("getClient", e);
		}
		return null;
	}

	public List<SearchResponse> getAllMultiResponseHits(
			MultiSearchResponse multiSearchResponse) {

		try {
			List<SearchResponse> result = new ArrayList<SearchResponse>();

			for (MultiSearchResponse.Item item : multiSearchResponse
					.getResponses()) {
				SearchResponse response = item.getResponse();
				result.add(response);
			}
			return result;

		} catch (Exception e) {
			logger.error("getAllMultiResponseHits", e);
		}
		return null;

	}
}
