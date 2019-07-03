package com.elastic.index;

import com.elastic.ClientFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * @author Marcos Gui
 * @date 2019-07-03
 */
public class IndexEsDocument {

  private static final Log log = LogFactory.getLog(IndexEsDocument.class);

  private static int BULK_ACTIONS = 10000;

  private static long BULK_SIZE = 5;

  private static int CONCURRENT_REQUESTS = 1;

  private static long FLASH_INTERVAL = 5000;

  private static long CONSTANT_BACKOFF_DELAY = 1000;

  private static int MAX_NUMBER_OF_RETRIES = 3;

  private static BulkProcessor processor;


  public void createIndex(String esIndex, List<Map<?, ?>> docList) {
    docList.forEach(doc -> {
      IndexRequest indexRequest = new IndexRequest(esIndex);
      indexRequest.source(doc, XContentType.JSON);
      try {
        ClientFactory.client.index(indexRequest, RequestOptions.DEFAULT);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  public void bulkIndex(String esIndex, List<Map<?,?>> docList) {
    docList.forEach( doc -> processor.add(new IndexRequest(esIndex).source(doc, XContentType.JSON)));
  }

  public void buildProcessor() {
    BulkProcessor.Listener listener = new Listener() {

      /**
       * Call back before the bulk is executed:
       */
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {

      }

      /**
       * Callback after a successful execution of bulk request
       */
      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

      }

      /**
       * Callback after a fail execution of bulk request
       */
      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
      }
    };
    BulkProcessor.Builder builder = BulkProcessor.builder((bulkRequest, bulkListener) -> ClientFactory.client
        .bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkListener), listener);
    builder.setBulkActions(BULK_ACTIONS)
        .setBulkSize(new ByteSizeValue(BULK_SIZE, ByteSizeUnit.MB))
        .setFlushInterval(TimeValue.timeValueMillis(FLASH_INTERVAL))
        .setConcurrentRequests(CONCURRENT_REQUESTS)
        .setBackoffPolicy(BackoffPolicy
            .constantBackoff(TimeValue.timeValueMillis(CONSTANT_BACKOFF_DELAY), MAX_NUMBER_OF_RETRIES));
    processor =  builder.build();
  }

  public void buildBulkOptions(int bulkActions, int concurrentRequests, long bulkSize, long flushInterval,
      long constantBackoffDelay, int maxNumberOfRetries) {
    BULK_ACTIONS = bulkActions;
    BULK_SIZE = bulkSize;
    FLASH_INTERVAL = flushInterval;
    CONCURRENT_REQUESTS = concurrentRequests;
    CONSTANT_BACKOFF_DELAY = constantBackoffDelay;
    MAX_NUMBER_OF_RETRIES = maxNumberOfRetries;
  }
}
