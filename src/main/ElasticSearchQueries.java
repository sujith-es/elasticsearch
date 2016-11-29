package main;

import java.util.List;

import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchQueries {

	private static final Logger logger = LoggerFactory
			.getLogger(ElasticSearchQueries.class);

	public SearchHits findInIndex(final String index, final String key,
			final String value) {

		try {
			System.out
					.println("Starting ElasticSearchQueries.findInIndex(-,-,-)");
			SearchResponse response = ElasticSearchUtil.getClient()
					.prepareSearch(index)
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.termQuery(key, value)).setFrom(0)
					.setSize(60).setExplain(true).execute().actionGet();

			System.out
					.println("---------------------response for ElasticSearchQueries.findInIndex()-----------------------");
			System.out.println(response.getHits().toString());

			return response.getHits();

		} catch (Exception e) {
			logger.error("ElasticSearchQueries", e);
		}
		return null;

	}

	public SearchHits findInCluster(String key, String value) {
		try {

			SearchResponse response = ElasticSearchUtil.getClient()
					.prepareSearch()
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.termQuery(key, value)).execute()
					.actionGet();

			System.out
					.println("---------------------response for ElasticSearchQueries.findInCluster()-----------------------");
			System.out.println(response.getHits().toString());

			return response.getHits();

		} catch (Exception e) {
			logger.error("findInCluster", e);
		}
		return null;

	}

	public SearchHits findInQuery(QueryBuilder builder) {

		try {

			SearchResponse response = ElasticSearchUtil.getClient()
					.prepareSearch()
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(builder).execute().actionGet();

			System.out
					.println("---------------------response for ElasticSearchQueries.findInQuery()-----------------------");
			System.out.println(response.getHits().toString());

			return response.getHits();

		} catch (Exception e) {
			logger.error("findInQuery", e);
		}
		return null;

	}

	public MultiSearchResponse mutliSearch(
			List<SearchRequestBuilder> searchRequestList) {

		try {
			MultiSearchRequestBuilder builder = ElasticSearchUtil.getClient()
					.prepareMultiSearch();

			for (SearchRequestBuilder requestBuilder : searchRequestList) {

				builder.add(requestBuilder);
			}

			System.out
					.println("---------------------response for ElasticSearchQueries.mutliSearch()-----------------------");
			System.out.println(builder.execute().actionGet().toString());

			return builder.execute().actionGet();

		} catch (Exception e) {
			// TODO: handle exception
		}
		return null;

	}

}
