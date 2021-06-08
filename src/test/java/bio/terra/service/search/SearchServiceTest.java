package bio.terra.service.search;

import bio.terra.common.category.Unit;
import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.model.SearchQueryRequest;
import bio.terra.model.SearchQueryResultModel;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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

        when(client.indices()).thenReturn(indicesClient);
    }

    @Test
    public void indexSnapshotTest() throws Exception {
        // Mock snapshot table data
        when(bigQueryPdao.getSnapshotTableData(snapshot, searchIndexRequest.getSql()))
            .thenReturn(values);

        // Mock index request
        when(client.indices()).thenReturn(indicesClient);
        when(client.indices().create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(new CreateIndexResponse(true, true, indexName));

        // Mock index response
        GetIndexResponse mockIndexResponse = mock(GetIndexResponse.class);
        when(mockIndexResponse.getIndices())
            .thenReturn(new String[]{indexName});
        when(client.indices().get(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
            .thenReturn(mockIndexResponse);

        SearchIndexModel searchIndexModel = service.indexSnapshot(snapshot, searchIndexRequest);
        assertEquals(indexName, searchIndexModel.getIndexSummary());
    }

    @Test
    public void querySnapshotTest() throws Exception {
        GetAliasesResponse mockResponse = mock(GetAliasesResponse.class);
        when(mockResponse.getAliases()).thenReturn(Map.of("0f14d0ab-9605-4a62-a9e4-5ed26688389b", new HashSet<>()));
        when(indicesClient.getAlias(any(GetAliasesRequest.class), any(RequestOptions.class)))
                .thenReturn(mockResponse);
        List<UUID> snaptshotIdsToQuery = Arrays.asList(
                UUID.fromString("0f14d0ab-9605-4a62-a9e4-5ed26688389b")
        );
        SearchHits mockHits = mock(SearchHits.class);
        SearchHit mockHit = mock(SearchHit.class);
        when(mockHits.getHits()).thenReturn(new SearchHit[]{mockHit});
        when(mockHit.getSourceAsMap()).thenReturn(Map.of("testKey", "testValue"));

        SearchResponse mockSearchResponse = mock(SearchResponse.class);
        when(mockSearchResponse.getHits()).thenReturn(mockHits);
        when(client.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(mockSearchResponse);

        SearchQueryResultModel actualResultModel =
                service.querySnapshot(new SearchQueryRequest().query("query"), snaptshotIdsToQuery, 0, 1);
        SearchQueryResultModel expectedResultModel = new SearchQueryResultModel();
        expectedResultModel.result(List.of(Map.of("testKey", "testValue")));

        assertEquals(expectedResultModel.getResult(), actualResultModel.getResult());
    }

    private SearchIndexRequest getSearchIndexRequest() {
        return new SearchIndexRequest().sql(sqlQuery);
    }

    private List<Map<String, Object>> getSnapshotTableData() {
        List<Map<String, Object>> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Instant now = Instant.now();
            String ts = String.format("%f", now.getEpochSecond() + now.getNano() / 1E9);
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
