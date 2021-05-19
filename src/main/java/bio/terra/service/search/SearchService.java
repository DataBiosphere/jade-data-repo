package bio.terra.service.search;

import bio.terra.common.exception.PdaoException;
import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
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

    private void createEmptyIndex(Snapshot snapshot) {
        String indexName = String.format("idx-%s", snapshot.getId());
        try {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName)
                .settings(Settings.builder()
                    .put("index.number_of_shards", 3));
            CreateIndexResponse response = null;
            response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new PdaoException("Error creating index", e);
        }
    }

    public SearchIndexModel indexSnapshot(Snapshot snapshot, SearchIndexRequest searchIndexRequest)
        throws InterruptedException {
        createEmptyIndex(snapshot);
        List<Map<String, Object>> values = bigQueryPdao.getSnapshotTableData(snapshot, searchIndexRequest.getSql());
        return new SearchIndexModel();
    }
}
