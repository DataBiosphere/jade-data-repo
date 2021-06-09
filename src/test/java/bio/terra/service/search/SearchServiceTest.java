package bio.terra.service.search;

import bio.terra.model.SearchIndexModel;
import bio.terra.common.category.Unit;
import bio.terra.model.SearchIndexRequest;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@Category(Unit.class)
public class SearchServiceTest {
    private static final String sqlQuery = "SELECT GENERATE_UUID() uuid, CURRENT_TIMESTAMP() as now" +
        " FROM UNNEST(GENERATE_ARRAY(1, 3));";

    private static final String indexName = "idx-mock";

    @Mock
    private BigQueryPdao bigQueryPdao;

    @Mock
    private RestHighLevelClient client;

    @Mock
    private IndicesClient indicesClient;

    private SearchService service;

    private SearchIndexRequest searchIndexRequest;
    private Snapshot snapshot;
    private List<Map<String, Object>> values;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
        service = new SearchService(bigQueryPdao, client);

        searchIndexRequest = getSearchIndexRequest();
        snapshot = getSnapshot();
        values = getSnapshotTableData();
    }

    @Test
    public void indexSnapshotTest() throws Exception {
        mockGetSnapshotTableData();
        mockIndexRequest();
        mockIndexResponse();
        SearchIndexModel searchIndexModel = service.indexSnapshot(snapshot, searchIndexRequest);
        assertEquals(indexName, searchIndexModel.getIndexSummary());
    }

    private void mockGetSnapshotTableData() throws InterruptedException {
        when(bigQueryPdao.getSnapshotTableData(snapshot, searchIndexRequest.getSql()))
            .thenReturn(values);
    }

    private void mockIndexRequest() throws Exception {
        when(client.indices()).thenReturn(indicesClient);
        when(client.indices().create(Mockito.any(CreateIndexRequest.class), Mockito.eq(RequestOptions.DEFAULT)))
            .thenReturn(new CreateIndexResponse(true, true, indexName));
    }

    private void mockIndexResponse() throws Exception {
        GetIndexResponse mockIndexResponse = Mockito.mock(GetIndexResponse.class);
        when(mockIndexResponse.getIndices())
            .thenReturn(new String[]{ indexName });
        when(client.indices().get(Mockito.any(GetIndexRequest.class), Mockito.eq(RequestOptions.DEFAULT)))
            .thenReturn(mockIndexResponse);
    }

    private SearchIndexRequest getSearchIndexRequest() {
        return new SearchIndexRequest().sql(sqlQuery);
    }

    private List<Map<String, Object>> getSnapshotTableData() {
        List<Map<String, Object>> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Instant now = Instant.now();
            String ts = String.format("%f", now.getEpochSecond() + now.getNano()/1E9);
            values.add(Map.of("uuid", UUID.randomUUID().toString(), "now", ts));
        }

        return values;
    }

    private Snapshot getSnapshot() {
        final String SNAPSHOT_NAME = "searchSnapshot";
        final String SNAPSHOT_DESCRIPTION = "searchSnapshotDescription";
        final String SNAPSHOT_DATA_PROJECT = "tdr-search-project";
        final String SNAPSHOT_TABLE_NAME = "tableA";

        final UUID snapshotId = UUID.randomUUID();
        final UUID snapshotTableId = UUID.randomUUID();
        final UUID profileId = UUID.randomUUID();
        final Instant createdDate = Instant.now();

        return new Snapshot()
            .id(snapshotId)
            .name(SNAPSHOT_NAME)
            .description(SNAPSHOT_DESCRIPTION)
            .createdDate(createdDate)
            .profileId(profileId)
            .projectResource(new GoogleProjectResource()
                .profileId(profileId)
                .googleProjectId(SNAPSHOT_DATA_PROJECT)
            )
            .snapshotTables(List.of(new SnapshotTable()
                .name(SNAPSHOT_TABLE_NAME)
                .id(snapshotTableId)
            ));
    }
}
