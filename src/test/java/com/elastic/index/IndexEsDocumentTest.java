package com.elastic.index;


import com.elastic.ClientFactory;
import java.io.IOException;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;

public class IndexEsDocumentTest {

  private RestHighLevelClient client;

  @Test
  public void createIndex() {
  }

  @Test
  public void bulkIndex() {
  }

  @Test
  public void buildProcessor() {
  }

  @Test
  public void buildBulkOptions() {
  }

  @Before
  public void setUp() throws Exception {
    ClientFactory.createClient("localhost", 9200);
    client = ClientFactory.client;
  }

  @Test
  public void testAggregation() throws IOException {
    SearchRequest searchRequest = new SearchRequest("f2a9dfd5-d40c-4db8-8dfd-7856d2c3a09c");
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.matchAllQuery());
    TermsAggregationBuilder terms = AggregationBuilders.terms("countrygroup").field("country.keyword");
    terms.subAggregation(AggregationBuilders.avg("zavg").field("z"));
    TermsAggregationBuilder terms2 = AggregationBuilders.terms("namegroup").field("name.keyword");
    terms2.subAggregation(AggregationBuilders.avg("xavg").field("x"));

    sourceBuilder.aggregation(terms);
    sourceBuilder.aggregation(terms2);
    searchRequest.source(sourceBuilder);
    SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
    Map<String, Aggregation> termsGroup = response.getAggregations().asMap();
    
  }
}