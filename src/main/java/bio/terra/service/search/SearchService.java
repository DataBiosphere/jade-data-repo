package bio.terra.service.search;

import bio.terra.app.utils.TimUtils;
import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.model.SearchQueryRequest;
import bio.terra.model.SearchQueryResultModel;
import bio.terra.service.filedata.google.bq.BigQueryDataResultModel;
import bio.terra.service.search.exception.SearchException;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
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

@Component
public class SearchService {

  private final BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private final RestHighLevelClient client;

  @Value("${elasticsearch.numShards}")
  private int NUM_SHARDS;

  @Autowired
  public SearchService(BigQuerySnapshotPdao bigQuerySnapshotPdao, RestHighLevelClient client) {
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.client = client;
  }

  private void validateSnapshotDataNotEmpty(List<BigQueryDataResultModel> values) {
    if (values.isEmpty()) {
      throw new SearchException("Snapshot data returned from SQL query is empty");
    }
  }

  private String uuidToIndexName(UUID id) {
    return String.format("idx-%s", id);
  }

  private String createEmptyIndex(Snapshot snapshot) {
    String indexName = uuidToIndexName(snapshot.getId());
    try {
      CreateIndexRequest createIndexRequest =
          new CreateIndexRequest(indexName)
              .settings(Settings.builder().put("index.number_of_shards", NUM_SHARDS));
      CreateIndexResponse response =
          client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
      if (!response.isAcknowledged()) {
        throw new SearchException("The index request was not acknowledged by one or more nodes");
      }
    } catch (IOException e) {
      throw new SearchException("Error creating index", e);
    }
    return indexName;
  }

  private void createIndexMapping(String indexName, List<BigQueryDataResultModel> values) {
    PutMappingRequest request = new PutMappingRequest(indexName);
    Map<String, Object> properties =
        values.get(0).getRowResult().keySet().stream()
            .collect(Collectors.toMap(Function.identity(), v -> Map.of("type", "text")));

    Map<String, Object> source = Map.of("properties", properties);
    request.source(source);
    try {
      client.indices().putMapping(request, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new SearchException("Error creating index mapping", e);
    }
  }

  private void addIndexData(String indexName, List<BigQueryDataResultModel> values) {
    BulkRequest request = new BulkRequest();
    values.stream()
        .forEach(
            v -> {
              var rr = v.getRowResult();
              request.add(
                  new IndexRequest(indexName)
                      .id(rr.get("uuid").toString())
                      .source(rr, XContentType.JSON));
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

    String sql = TimUtils.encodeSqlColumns(searchIndexRequest.getSql());
    List<BigQueryDataResultModel> values =
        bigQuerySnapshotPdao.getSnapshotTableUnsafe(snapshot, sql);

    validateSnapshotDataNotEmpty(values);
    String indexName = createEmptyIndex(snapshot);
    createIndexMapping(indexName, values);
    addIndexData(indexName, values);
    return getIndexSummary(indexName);
  }

  private Set<String> getValidIndexes() {
    try {
      GetAliasesResponse response =
          client.indices().getAlias(new GetAliasesRequest(), RequestOptions.DEFAULT);
      return response.getAliases().keySet();
    } catch (IOException e) {
      throw new SearchException("Error getting indexes", e);
    }
  }

  public SearchQueryResultModel querySnapshot(
      SearchQueryRequest searchQueryRequest,
      Collection<UUID> snapshotIdsToQuery,
      int offset,
      int limit) {
    var searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.from(offset);
    searchSourceBuilder.size(limit);
    // see
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wrapper-query.html
    String query = TimUtils.encodeQueryFields(searchQueryRequest.getQuery());
    WrapperQueryBuilder wrapperQuery = QueryBuilders.wrapperQuery(query);
    searchSourceBuilder.query(wrapperQuery);

    Set<String> validIndexes = getValidIndexes();

    String[] indicesToQuery =
        snapshotIdsToQuery.stream()
            .map(this::uuidToIndexName)
            .filter(validIndexes::contains)
            .toArray(String[]::new);

    var searchRequest = new SearchRequest(indicesToQuery, searchSourceBuilder);

    final SearchResponse elasticResponse;
    try {
      elasticResponse = client.search(searchRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new SearchException("Error completing search request", e);
    }
    SearchHits hits = elasticResponse.getHits();
    var response = hitsToMap(hits);

    SearchQueryResultModel result = new SearchQueryResultModel();
    // do we want to include info on the index/snapshot the response came from?
    result.setResult(response);
    return result;
  }

  private List<Map<String, String>> hitsToMap(SearchHits hits) {
    List<Map<String, String>> response = new ArrayList<>();
    for (SearchHit hit : hits) {
      Map<String, String> hitsMap = new HashMap<>();
      for (Map.Entry<String, Object> entry : hit.getSourceAsMap().entrySet()) {
        String key = TimUtils.decode(entry.getKey());
        String value = (String) entry.getValue();
        hitsMap.put(key, value);
      }
      response.add(hitsMap);
    }
    return response;
  }
}
