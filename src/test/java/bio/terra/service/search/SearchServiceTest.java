package bio.terra.service.search;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.utils.TimUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.model.SearchQueryRequest;
import bio.terra.model.SearchQueryResultModel;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDataResultModel;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SearchServiceTest {
  private static final String timPropertyName = "example:identifier.now";
  private static final String timEncodedName = TimUtils.encode(timPropertyName);

  private static final String sqlQuery =
      String.format(
          "SELECT GENERATE_UUID() uuid, CURRENT_TIMESTAMP() AS [%s]"
              + " FROM UNNEST(GENERATE_ARRAY(1, 3));",
          timPropertyName);

  private static final String searchQuery =
      String.format("{\"query_string\": {\"query\": \"([%s]:0)\"}}", timPropertyName);

  private static final String indexName = "idx-mock";

  @Mock private BigQuerySnapshotPdao bigQuerySnapshotPdao;

  @Mock private RestHighLevelClient client;

  @Mock private IndicesClient indicesClient;

  private SearchService service;

  private SearchIndexRequest searchIndexRequest;
  private Snapshot snapshot;
  private List<BigQueryDataResultModel> values;

  @Before
  public void setup() throws Exception {
    service = new SearchService(bigQuerySnapshotPdao, client);

    searchIndexRequest = getSearchIndexRequest();
    snapshot = getSnapshot();
    values = getSnapshotTableData();

    when(client.indices()).thenReturn(indicesClient);
  }

  @Test
  public void timColumnEncodingTest() {
    String expectedSql =
        String.format(
            "SELECT GENERATE_UUID() uuid, CURRENT_TIMESTAMP() AS %s"
                + " FROM UNNEST(GENERATE_ARRAY(1, 3));",
            timEncodedName);
    String actualSql = TimUtils.encodeSqlColumns(sqlQuery);
    assertEquals(expectedSql, actualSql);
  }

  @Test
  public void timFieldEncodingTest() {
    String expectedQuery =
        String.format("{\"query_string\": {\"query\": \"(%s:0)\"}}", timEncodedName);
    String actualQuery = TimUtils.encodeQueryFields(searchQuery);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void indexSnapshotTest() throws Exception {
    // Mock snapshot table data
    when(bigQuerySnapshotPdao.getSnapshotTableUnsafe(any(Snapshot.class), any(String.class)))
        .thenReturn(values);

    // Mock index request
    when(client.indices()).thenReturn(indicesClient);
    when(client.indices().create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(new CreateIndexResponse(true, true, indexName));

    // Mock index response
    GetIndexResponse mockIndexResponse = mock(GetIndexResponse.class);
    when(mockIndexResponse.getIndices()).thenReturn(new String[] {indexName});
    when(client.indices().get(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mockIndexResponse);

    SearchIndexModel searchIndexModel = service.indexSnapshot(snapshot, searchIndexRequest);
    assertEquals(indexName, searchIndexModel.getIndexSummary());
  }

  @Test
  public void querySnapshotTest() throws Exception {
    String testId = "0f14d0ab-9605-4a62-a9e4-5ed26688389b";

    GetAliasesResponse mockResponse = mock(GetAliasesResponse.class);
    when(mockResponse.getAliases()).thenReturn(Map.of(String.format("idx-%s", testId), Set.of()));
    when(indicesClient.getAlias(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);
    SearchHits mockHits = mock(SearchHits.class);
    SearchHit mockHit = mock(SearchHit.class);
    when(mockHits.iterator()).thenReturn(Arrays.stream(new SearchHit[] {mockHit}).iterator());
    when(mockHit.getSourceAsMap()).thenReturn(Map.of(timEncodedName, "0"));

    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    when(mockSearchResponse.getHits()).thenReturn(mockHits);
    when(client.search(any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSearchResponse);

    List<UUID> snapshotIdsToQuery = List.of(UUID.fromString(testId));
    SearchQueryResultModel actualResultModel =
        service.querySnapshot(new SearchQueryRequest().query("query"), snapshotIdsToQuery, 0, 1);
    SearchQueryResultModel expectedResultModel = new SearchQueryResultModel();
    expectedResultModel.result(List.of(Map.of(timPropertyName, "0")));

    assertEquals(expectedResultModel.getResult(), actualResultModel.getResult());
  }

  private SearchIndexRequest getSearchIndexRequest() {
    return new SearchIndexRequest().sql(sqlQuery);
  }

  private List<BigQueryDataResultModel> getSnapshotTableData() {
    List<BigQueryDataResultModel> values = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Instant now = Instant.now();
      String ts = String.format("%f", now.getEpochSecond() + now.getNano() / 1E9);
      BigQueryDataResultModel result = new BigQueryDataResultModel();
      result
          .rowResult(Map.of("uuid", UUID.randomUUID().toString(), timEncodedName, ts))
          .filteredCount(3)
          .totalCount(3);
      values.add(result);
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
        .projectResource(
            new GoogleProjectResource().profileId(profileId).googleProjectId(SNAPSHOT_DATA_PROJECT))
        .snapshotTables(List.of(new SnapshotTable().name(SNAPSHOT_TABLE_NAME).id(snapshotTableId)));
  }
}
