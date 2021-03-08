package com.ttajun.es.conn;

import com.ttajun.es.common.Constants;
import com.ttajun.es.common.VehicleType;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ttajun.es.common.VehicleType.GLOBIZ;
import static com.ttajun.es.common.VehicleType.KTME;

public class EsHighRestConnector {
    private static final Logger log = LoggerFactory.getLogger(EsHighRestConnector.class);

    private static final RestHighLevelClient client;
    static {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(Constants.ES_HOST, Constants.ES_REST_PORT, Constants.ES_PROTOCOL))
        );
    }

    private VehicleType getVehicle(String index) {
        VehicleType ret = null;
        if(index.equals(KTME.getName())) ret = KTME;
        else if(index.equals(GLOBIZ.getName())) ret = GLOBIZ;
        return ret;
    }

    public void updateData(String index, Map<String, String> data) {
        VehicleType type = getVehicle(index);
        if(type == null) return;

        BulkRequest bulkRequest = new BulkRequest();
        for(String str : data.keySet()) {
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("u_defect_prob", data.get(str));
            UpdateRequest request = new UpdateRequest(type.getIndex(), "doc", str).doc(jsonMap);
            bulkRequest.add(request);
        }

        try {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

            for(BulkItemResponse bulkItemResponse : bulkResponse) {
                if(bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    log.info(failure.getId() + ": " + failure.getMessage());
                }
            }
        } catch (IOException e) {
            e.getCause();
        }
    }

    public List<Map<String, Object>> getData(String index, String train, String car, String part, String start, String end) {
        List<Map<String, Object>> ret = new ArrayList<>();

        VehicleType type = getVehicle(index);
        if(type == null) return ret;

        BoolQueryBuilder boolBuilder = new BoolQueryBuilder();
        boolBuilder.must(QueryBuilders.termQuery(type.getTrain(), train));
        boolBuilder.must(QueryBuilders.termQuery(type.getCar(), car));
        boolBuilder.must(QueryBuilders.termQuery(type.getPart(), part));
        boolBuilder.must(QueryBuilders.rangeQuery(type.getTime()).gt(start).lt(end));

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(boolBuilder);
        //builder.from(0);
        builder.size(10);

        SearchRequest request = new SearchRequest(type.getIndex());
        request.source(builder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //RestStatus status = response.status();
            //log.info("status: " + status.toString());
            TimeValue took = response.getTook();
            log.info("took: " + took);

            SearchHits hits = response.getHits();
            long totalHits = hits.getTotalHits();
            log.info("total hits: " + totalHits);

            SearchHit[] searchHits = hits.getHits();
            log.info("searchHits size: " + searchHits.length);
            for (SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                sourceAsMap.put("docID", hit.getId());
                /*
                log.info("sourceAsMap...");
                for (String str : sourceAsMap.keySet()) {
                    log.info("{} : {}", str, sourceAsMap.get(str));
                }
                 */
                ret.add(sourceAsMap);
            }
        } catch (IOException e) {
            e.getCause();
        }

        return ret;
    }

    public String getFieldList(String index) {
        VehicleType type = getVehicle(index);
        if(type == null) return "";

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.size(1);

        SearchRequest request = new SearchRequest(type.getIndex());
        request.source(builder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                return String.join(",", sourceAsMap.keySet());
            }
        } catch (IOException e) {
            e.getCause();
        }

        return "";
    }

    public String getTrainList(String index) {
        VehicleType type = getVehicle(index);
        if(type == null) return "";

        Map<String, String> map = new HashMap<>();
        map.put("", "");

        return getFieldDataList(type.getIndex(), map, type.getTrain());
    }

    public String getCarList(String index, String train) {
        VehicleType type = getVehicle(index);
        if(type == null) return "";

        Map<String, String> map = new HashMap<>();
        map.put(type.getTrain(), train);

        return getFieldDataList(type.getIndex(), map, type.getCar());
    }

    public String getPartList(String index, String train, String car) {
        VehicleType type = getVehicle(index);
        if(type == null) return "";

        Map<String, String> map = new HashMap<>();
        map.put(type.getTrain(), train);
        map.put(type.getCar(), car);

        return getFieldDataList(type.getIndex(), map, type.getPart());
    }

    private String getFieldDataList(String index, Map<String, String> query, String target) {
        String aggName = "FieldDataList";
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolBuilder = new BoolQueryBuilder();
        for(String str : query.keySet()) {
            if(str.equals("")) {
                builder.query(QueryBuilders.matchAllQuery());
                break;
            } else {
                boolBuilder.must(QueryBuilders.termQuery(str, query.get(str)));
                builder.query(boolBuilder);
                //builder.query(QueryBuilders.termQuery(str, query.get(str)));
            }
        }

        // 데이터는 가져오지 않고 집계 결과만 가져옴.
        builder.fetchSource(false);

        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms(aggName).field(target);
        builder.aggregation(aggBuilder);

        SearchRequest request = new SearchRequest(index);
        request.source(builder);

        try {
            List<String> trains = new ArrayList<>();
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            Aggregations aggregations = response.getAggregations();
            Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
            Terms ATV_sum = (Terms) aggregationMap.get(aggName);
            List<? extends Terms.Bucket> buckets = ATV_sum.getBuckets();
            for(Terms.Bucket b : buckets) {
                trains.add(b.getKeyAsString());
            }
            return String.join(",", trains);
        } catch (IOException e) {
            e.getCause();
        }

        return "";
    }

    public void searchAll(String index) {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.termQuery("Car No", "763171"));
        String strFrom = "07/05/2019 00:00:00";
        String strTo = "07/05/2019 11:55:55";
        builder.query(QueryBuilders.rangeQuery("TIME.keyword")
                .from(strFrom)
                .to(strTo));
        builder.from(0);
        builder.size(10);
        builder.sort(new FieldSortBuilder("TIME.keyword").order(SortOrder.ASC));

        // 1) header
        //String[] includeFields = new String[] {"ATV", "Car No"};
        //String[] excludeFields = new String[] {"gaion_defect_prob"};
        //builder.fetchSource(includeFields, excludeFields);

        SearchRequest request = new SearchRequest(index);
        request.source(builder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            RestStatus status = response.status();
            log.info("status: " + status.toString());
            TimeValue took = response.getTook();
            log.info("took: " + took);

            SearchHits hits = response.getHits();
            long totalHits = hits.getTotalHits();
            log.info("Hits...");
            log.info("total hits: " + totalHits);

            boolean isFirst = true;
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                if(isFirst) {
                    log.info("index({})", hit.getIndex());

                    String sourceAsString = hit.getSourceAsString();
                    log.info("sourceAsString: " + sourceAsString);

                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    log.info("sourceAsMap...");
                    for (String str : sourceAsMap.keySet()) {
                        log.info("{} : {}", str, sourceAsMap.get(str));
                    }

                    String documentTitle = (String) sourceAsMap.get("title");
                    log.info("documentTitle: " + documentTitle);

                    List<Object> users = (List<Object>) sourceAsMap.get("user");
                    if(users != null) {
                        log.info("users...");
                        for (Object obj : users) {
                            log.info(obj.toString());
                        }
                    }

                    Map<String, Object> innerObject = (Map<String, Object>) sourceAsMap.get("innerObject");
                    if (innerObject != null) {
                        log.info("innerObject...");
                        for (String str : innerObject.keySet()) {
                            log.info("{} : {}", str, innerObject.get(str));
                        }
                    }

                    isFirst = false;
                }
                log.info(hit.getId());
            }
        } catch (IOException e) {
            e.getCause();
        }
    }

    public void searchTemplate(String index) {
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest(index));
        request.setScriptType(ScriptType.INLINE);

        /*
        request.setScript(
                "{" +
                    "\"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
                    "\"aggs\": { " +
                        "\"ATV_sum\" : { " +
                            "\"terms\" : { " +
                                "\"{{field_1}}\" : \"{{value_1}}\"" +
                            "}" +
                        "}" +
                    "}," +
                    "\"size\" : \"{{size}}\"" +
                "}");
         */
        //"\"query\": { }," +
        request.setScript(
            "{" +
                "\"aggs\": { " +
                    "\"train_sum\" : { " +
                        "\"terms\" : { " +
                            "\"{{field}}\" : \"{{value}}\"" +
                        "}" +
                    "}" +
                "}," +
                "\"size\" : \"{{size}}\"" +
            "}");

        Map<String, Object> param = new HashMap<>();
        param.put("field", "field");
        param.put("value", "Car No.keyword");
        param.put("size", 0);
        request.setScriptParams(param);

        try {
            SearchTemplateResponse searchTemplateResponse = client.searchTemplate(request, RequestOptions.DEFAULT);
            SearchResponse response = searchTemplateResponse.getResponse();
            TimeValue took = response.getTook();
            log.info("took: " + took);

            SearchHits hits = response.getHits();
            long totalHits = hits.getTotalHits();
            log.info("total hits: " + totalHits);

            boolean isFirst = true;
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                if(isFirst) {
                    String sourceAsString = hit.getSourceAsString();
                    log.info("sourceAsString: " + sourceAsString);

                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    log.info("sourceAsMap...");
                    for (String str : sourceAsMap.keySet()) {
                        log.info("{} : {}", str, sourceAsMap.get(str));
                    }
                    isFirst = false;
                }
                //log.info(hit.getId());
            }

            Aggregations aggregations = response.getAggregations();
            Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
            Terms ATV_sum = (Terms) aggregationMap.get("train_sum");
            List<? extends Terms.Bucket> buckets = ATV_sum.getBuckets();
            for(Terms.Bucket b : buckets) {
                log.info("getDocCount: " + b.getDocCount());
                log.info("getKeyAsString: " + b.getKeyAsString());
            }
        } catch (IOException e) {
            e.getCause();
        }
    }

    public void searchWithAgg(String index) {
        String aggName = "trainList";
        String fieldName = "Car No.keyword";

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.fetchSource(false);

        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms(aggName).field(fieldName);
        builder.aggregation(aggBuilder);

        SearchRequest request = new SearchRequest(index);
        request.source(builder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            Aggregations aggregations = response.getAggregations();
            Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
            Terms ATV_sum = (Terms) aggregationMap.get(aggName);
            List<? extends Terms.Bucket> buckets = ATV_sum.getBuckets();
            for(Terms.Bucket b : buckets) {
                log.info("getDocCount: " + b.getDocCount());
                log.info("getKeyAsString: " + b.getKeyAsString());
            }
        } catch (IOException e) {
            e.getCause();
        }
    }

    public void scrollAll(String index) {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.size(1000);

        request.source(builder);
        request.scroll(TimeValue.timeValueMinutes(1L));

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            String scrollId = response.getScrollId();
            SearchHits hits = response.getHits();
            log.info("scroll id: " + scrollId);
            long totalHits = hits.getTotalHits();
            log.info("total hits: " + totalHits);
            log.info("headers: " + hits.getCollapseField());

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(30));
            SearchResponse scrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = scrollResponse.getScrollId();
            hits = scrollResponse.getHits();
        } catch (IOException e) {
            e.getCause();
        }
    }
}
