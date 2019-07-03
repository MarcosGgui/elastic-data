package com.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ClientFactory {

  private static final String SCHEMA = "http";

  public static RestHighLevelClient client;

  public static void createClient(String hostName, int port) {
    client = new RestHighLevelClient(RestClient.builder(
        new HttpHost(hostName, port, SCHEMA)
    ));
  }

}
