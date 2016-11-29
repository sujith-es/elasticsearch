package test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import main.ElastiSearchService;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ElasticSearchCRUDTest extends BaseTest {

	ElastiSearchService elasticSearchService = null;

	@BeforeTest
	public void init() {
		elasticSearchService = ElastiSearchService.getInstance();
	}

	@Test(enabled = true, priority = 1)
	public void createIndex() throws IOException {
		XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
		Map<String, Object> data = new HashMap<String, Object>();

		data.put("firstname", "sujith");
		data.put("lastname", "surendran");
		data.put("email", "es.sujit@gmail.com");

		jsonBuilder.map(data);
		IndexResponse response = elasticSearchService.createIndex("users",
				"user", "1", jsonBuilder);

		Assert.assertSame(response.isCreated(), true);

	}

	@Test(enabled = true, priority = 2)
	public void findDocumentByIndex() {

		GetResponse response = elasticSearchService.findDocumentByIndex(
				"users", "user", "1");

		Map<String, Object> source = response.getSource();

		System.out.println("------------------------------");
		System.out.println("Index:	" + response.getIndex());
		System.out.println("Type:	" + response.getType());
		System.out.println("Id:	" + response.getId());
		System.out.println("version:	" + response.getVersion());
		System.out.println("getFields:	" + response.getFields());
		System.out.println(source);
		System.out.println("------------------------------");
		Assert.assertSame(response.isExists(), true);
	}

	@Test(enabled = true, priority = 3)
	public void findDocument() {

		SearchResponse response = elasticSearchService.findDocument("users",
				"user", "email", "es.sujit@gmail.com");

		SearchHit[] results = response.getHits().getHits();
		System.out.println("current results: " + results.length);

		for (SearchHit hit : results) {
			System.out.println("--------------HIT----------------");
			System.out.println("Index: " + hit.getIndex());
			System.out.println("Type: " + hit.getType());
			System.out.println("Id: " + hit.getId());
			System.out.println("Version: " + hit.getVersion());
			Map<String, Object> result = hit.getSource();
			System.out.println(result);
		}

		Assert.assertSame(response.getHits().totalHits() > 0, true);
	}

	@Test(enabled = true, priority = 4)
	public void updateDocument() throws IOException {
		XContentBuilder builder = XContentFactory.jsonBuilder();

		Map<String, Object> data = new HashMap<String, Object>();
		data.put("lastname", "Test");
		data.put("firstname", "Ekkattil Sujith");

		builder.map(data);
		UpdateResponse updateResponse = elasticSearchService.updateIndex(
				"users", "user", "1", builder);
		SearchResponse response = elasticSearchService.findDocument("users",
				"user", "lastname", "Test");

		Assert.assertSame(response.getHits().totalHits() > 0, true);

	}

	@Test(enabled = true, priority = 5)
	public void removeDocument() {
		DeleteResponse response = elasticSearchService.removeDocument("users",
				"user", "1");
		Assert.assertSame(response.isFound(), true);
	}

}
