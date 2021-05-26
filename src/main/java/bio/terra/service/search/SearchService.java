package bio.terra.service.search;

import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.service.search.exception.SearchException;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class SearchService {

    private final BigQueryPdao bigQueryPdao;
    private final RestHighLevelClient client;

    @Autowired
    public SearchService(BigQueryPdao bigQueryPdao) {
        // needs to be moved into own class with config
        final RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200));
        this.bigQueryPdao = bigQueryPdao;
        this.client = new RestHighLevelClient(builder);
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
                    .put("index.number_of_shards", 3));
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
        Map<String, Object> properties = new HashMap<>();
        values.get(0).forEach((key, val) -> {
            properties.put(key, Map.of("type", "text"));
        });
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
}
