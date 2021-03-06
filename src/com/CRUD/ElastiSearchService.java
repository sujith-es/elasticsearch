package com.CRUD;

import java.io.IOException;
import java.util.Date;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class ElastiSearchService {

	private static Logger logger = LoggerFactory
			.getLogger(ElasticsearchSecurityException.class);

	private static ElastiSearchService instance = null;

	public static ElastiSearchService getInstance() {

		if (instance == null) {
			instance = new ElastiSearchService();
		}
		return instance;
	}

	public boolean isIndexExists(String indexId) {

		try {

			if (ElasticSearchUtil.getClient().admin().indices()
					.prepareExists(indexId).execute().get().isExists()) {

				return true;
			}

		} catch (Exception e) {
			logger.error("isIndexExists()", e);
		}
		return false;
	}

	public IndexResponse createIndex(String index, String type, String id,
			XContentBuilder jsonData) {

		try {
			IndexResponse response = null;

			response = ElasticSearchUtil.getClient()
					.prepareIndex(index, type, id).setSource(jsonData).get();

			Thread.sleep(2000);
			return response;

		} catch (Exception e) {
			logger.error("createIndex()", e);
		}

		return null;
	}

	public UpdateResponse updateIndex(String index, String type, String id,
			XContentBuilder jsonData) {

		try {
			UpdateResponse response = null;

			response = ElasticSearchUtil.getClient()
					.prepareUpdate(index, type, id).setDoc(jsonData).get();
			Thread.sleep(2000);
			return response;

		} catch (Exception e) {
			logger.error("updateIndex", e);
		}

		return null;

	}

	public DeleteResponse removeDocument(String index, String type, String id) {
		DeleteResponse response = null;
		try {
			response = ElasticSearchUtil.getClient()
					.prepareDelete(index, type, id).execute().get();

			return response;

		} catch (Exception e) {
			logger.error("removeDocument", e);
		}
		return null;

	}

	public GetResponse findDocumentByIndex(String index, String type, String id) {

		try {

			GetResponse response = ElasticSearchUtil.getClient()
					.prepareGet(index, type, id).get();

			return response;

		} catch (Exception e) {
			logger.error("findDocumentByIndex", e);
		}
		return null;

	}

	public SearchResponse findDocument(String index, String type, String field,
			String value) {
		try {
			QueryBuilder queryBuilder = new MatchQueryBuilder(field, value);
			SearchResponse response = ElasticSearchUtil.getClient()
					.prepareSearch(index).setTypes(type)
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(queryBuilder).setFrom(0).setSize(60)
					.setExplain(true).execute().get();

			SearchHit[] results = response.getHits().getHits();

			return response;

		} catch (Exception e) {
			logger.error("findDocument", e);
		}

		return null;

	}

	public void bulkInsert() throws IOException {
		
		//TODO: should remove below hard coding and pass List<T>

		BulkRequestBuilder bulkRequest = ElasticSearchUtil.getClient()
				.prepareBulk();

		// either use client#prepare, or use Requests# to directly build
		// index/delete requests
		bulkRequest.add(ElasticSearchUtil
				.getClient()
				.prepareIndex("twitter", "tweet", "1")
				.setSource(
						jsonBuilder().startObject().field("user", "sujith")
								.field("postDate", new Date())
								.field("message", "trying out Elasticsearch")
								.endObject()));

		bulkRequest.add(ElasticSearchUtil
				.getClient()
				.prepareIndex("twitter", "tweet", "2")
				.setSource(
						jsonBuilder().startObject().field("user", "hima")
								.field("postDate", new Date())
								.field("message", "another post").endObject()));

		BulkResponse bulkResponse = bulkRequest.get();
		if (bulkResponse.hasFailures()) {
			// process failures by iterating through each bulk response item
		}

	}
}
