package bio.terra.service.search;

import bio.terra.common.category.Unit;
import bio.terra.model.SearchIndexRequest;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.when;

@Category(Unit.class)
public class SearchTest {
    private static final String sqlQuery = "SELECT GENERATE_UUID() uuid, CURRENT_TIMESTAMP() as now FROM UNNEST" +
        "(GENERATE_ARRAY(1, 3));";

    @Mock
    private BigQueryPdao bigQueryPdao;

    @Mock
    private RestHighLevelClient client;

    private SearchService service;

    private SearchIndexRequest searchIndexRequest;
    private Snapshot snapshot;
    private List<Map<String, Object>> values;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        service = new SearchService(bigQueryPdao, client);

        searchIndexRequest = getSearchIndexRequest();
        snapshot = getSnapshot();
        values = getSnapshotTableData();
    }

    @Test
    public void indexSnapshotTest() throws Exception {
        mockGetSnapshotTableData();
        //service.indexSnapshot(snapshot, searchIndexRequest);
    }

    private void mockGetSnapshotTableData() throws InterruptedException {
        when(bigQueryPdao.getSnapshotTableData(snapshot, searchIndexRequest.getSql()))
            .thenReturn(values);
    }

    private SearchIndexRequest getSearchIndexRequest() {
        return new SearchIndexRequest().sql(sqlQuery);
    }

    private List<Map<String, Object>> getSnapshotTableData() {
        List<Map<String, Object>> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("uuid", UUID.randomUUID().toString());
            row.put("now", Instant.now().toString());
            values.add(row);
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
