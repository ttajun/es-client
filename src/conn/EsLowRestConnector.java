package com.ttajun.es.conn;

import com.ttajun.es.common.Constants;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class EsLowRestConnector {
    private static final Logger log = LoggerFactory.getLogger(EsLowRestConnector.class);

    private static final RestClient restClient;
    static {
        //restClient = RestClient.builder(
        //    new HttpHost(Constants.ES_HOST, Constants.ES_REST_PORT, Constants.ES_PROTOCOL)).build();

        RestClientBuilder builder = RestClient.builder(new HttpHost(Constants.ES_HOST, Constants.ES_REST_PORT))
            .setRequestConfigCallback(
                new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
                        return builder
                                .setConnectTimeout(5000)
                                .setSocketTimeout(60000);
                    }
                }
            );
        restClient = builder.build();
    }

    public void reqGetAll() {
        Request request = new Request("GET", "/");
        try {
            Response response = restClient.performRequest(request);
            RequestLine requestLine = response.getRequestLine();
            log.info("method ({}), uri ({})", requestLine.getMethod(), requestLine.getUri());
            String body = EntityUtils.toString(response.getEntity());
            log.info(body);
        } catch (IOException e) {
            e.getCause();
        }
    }
}
