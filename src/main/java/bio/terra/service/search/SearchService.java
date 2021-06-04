package bio.terra.service.search;

import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.service.search.exception.SearchException;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.google.BigQueryPdao;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import bio.terra.model.SearchQueryRequest;
import bio.terra.model.SearchQueryResultModel;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class SearchService {

    private final BigQueryPdao bigQueryPdao;
    private final RestHighLevelClient client;

    @Value("${elasticsearch.numShards}")
    private static int NUM_SHARDS;

    @Autowired
    public SearchService(BigQueryPdao bigQueryPdao, RestHighLevelClient client) {
        this.bigQueryPdao = bigQueryPdao;
        // injected from ElasticSearchRestClientConfigurations.java
        this.client = client;
    }

    private void validateSnapshotDataNotEmpty(List<Map<String, Object>> values) {
        if (values.isEmpty()) {
            throw new SearchException("Snapshot data returned from SQL query is empty");
        }
    }

    private String createEmptyIndex(Snapshot snapshot) {
        String indexName = String.format("idx-%s", snapshot.getId());
        try {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName)
                .settings(Settings.builder()
                    .put("index.number_of_shards", NUM_SHARDS));
            CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            if (!response.isAcknowledged()) {
                throw new SearchException("The index request was not acknowledged by one or more nodes");
            }
        } catch (IOException e) {
            throw new SearchException("Error creating index", e);
        }
        return indexName;
    }

    private void createIndexMapping(String indexName, List<Map<String, Object>> values) {
        PutMappingRequest request = new PutMappingRequest(indexName);
        Map<String, Object> properties =
            values.get(0).keySet().stream()
                .collect(Collectors.toMap(Function.identity(), v -> Map.of("type", "text")));

        Map<String, Object> source = Map.of("properties", properties);
        request.source(source);
        try {
            client.indices().putMapping(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchException("Error creating index mapping", e);
        }
    }

    private void addIndexData(String indexName, List<Map<String, Object>> values) {
        BulkRequest request = new BulkRequest();
        values.forEach(v -> {
            request.add(new IndexRequest(indexName)
                .id(v.get("uuid").toString())
                .source(v, XContentType.JSON)
            );
        });
        try {
            client.bulk(request, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new SearchException("Error indexing data", e);
        }
    }

    private SearchIndexModel getIndexSummary(String indexName) {
        GetIndexRequest request = new GetIndexRequest(indexName);
        try {
            GetIndexResponse getIndexResponse = client.indices().get(request, RequestOptions.DEFAULT);
            SearchIndexModel searchIndexModel = new SearchIndexModel();
            searchIndexModel.setIndexSummary(getIndexResponse.getIndices()[0]);
            return searchIndexModel;
        } catch (IOException e) {
            throw new SearchException("Error getting index summary", e);
        }
    }

    public SearchIndexModel indexSnapshot(Snapshot snapshot, SearchIndexRequest searchIndexRequest)
        throws InterruptedException {
        // TODO: add streaming version of this mechanism instead of loading everything into memory
        List<Map<String, Object>> values = bigQueryPdao.getSnapshotTableData(snapshot, searchIndexRequest.getSql());
        validateSnapshotDataNotEmpty(values);
        String indexName = createEmptyIndex(snapshot);
        createIndexMapping(indexName, values);
        addIndexData(indexName, values);
        return getIndexSummary(indexName);
    }
    public SearchQueryResultModel querySnapshot(
            SearchQueryRequest searchQueryRequest, List<String> indicesToQuery,
            int offset, int limit
    ) {
        //todo: move to bean for configuration, make injectable
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(offset);
        searchSourceBuilder.size(limit);
        // see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wrapper-query.html
        WrapperQueryBuilder wrapperQuery = QueryBuilders.wrapperQuery(searchQueryRequest.getQuery());
        searchSourceBuilder.query(wrapperQuery);

        SearchRequest searchRequest = new SearchRequest(indicesToQuery.toArray(new String[0]), searchSourceBuilder);

        final SearchResponse elasticResponse;
        try {
            elasticResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchException("Error completing search request", e);
        }
        SearchHits hits = elasticResponse.getHits();
        var response = hitsToMap(hits);

        SearchQueryResultModel result = new SearchQueryResultModel();
        //do we want to include info on the index/snapshot the response came from?
        result.setResult(response);
        return result;

    }

    private List<Map<String, String>> hitsToMap(SearchHits hits) {
        List<Map<String, String>> response = new ArrayList<>();
        for (SearchHit hit : hits) {
            Map<String, String> hitsMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : hit.getSourceAsMap().entrySet()) {
                String key = entry.getKey();
                String value = (String) entry.getValue();
                hitsMap.put(key, value);
            }
            response.add(hitsMap);
        }
        return response;
    }
}
