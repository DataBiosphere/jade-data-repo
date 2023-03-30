package bio.terra.tanagra.query.azure;

import static bio.terra.service.filedata.azure.AzureSynapsePdao.getDataSourceName;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.tanagra.query.ColumnHeaderSchema;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.underlay.datapointer.AzureDataset;
import com.google.cloud.bigquery.TableId;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** A {@link QueryExecutor} for Google BigQuery. */
@Service
public class AzureExecutor implements QueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(AzureExecutor.class);

  private final MetadataDataAccessUtils metadataDataAccessUtils;
  private final AzureSynapsePdao azureSynapsePdao;
  private final DatasetService datasetService;

  public AzureExecutor(
      MetadataDataAccessUtils metadataDataAccessUtils,
      AzureSynapsePdao azureSynapsePdao,
      DatasetService datasetService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
    this.metadataDataAccessUtils = metadataDataAccessUtils;
  }

  private static Optional<Object> getField(String name, ResultSet resultSet) {
    try {
      return Optional.ofNullable(resultSet.getObject(name));
    } catch (SQLException e) {
      return Optional.empty();
    }
  }

  public void setupAccess(AzureDataset azureDataset) {
    AuthenticatedUserRequest userRequest =
        AuthenticatedUserRequest.builder()
            .setSubjectId("ignored")
            .setToken("ignored")
            .setEmail(azureDataset.getUserName())
            .build();

    Dataset dataset = datasetService.retrieve(azureDataset.getDatasetId());

    AccessInfoModel accessInfoModel =
        metadataDataAccessUtils.accessInfoFromDataset(dataset, userRequest);
    String credName = AzureSynapsePdao.getCredentialName(dataset.getId(), userRequest.getEmail());
    String datasourceName = getDataSourceName(dataset.getId(), userRequest.getEmail());
    String metadataUrl =
        "%s?%s"
            .formatted(
                accessInfoModel.getParquet().getUrl(), accessInfoModel.getParquet().getSasToken());

    try {
      azureSynapsePdao.getOrCreateExternalDataSource(metadataUrl, credName, datasourceName);
    } catch (Exception e) {
      throw new RuntimeException("Could not configure external datasource", e);
    }
  }

  @Override
  public QueryResult execute(QueryRequest queryRequest) {

    String sql = renderSQL(queryRequest.query());
    LOGGER.info("Running SQL against {}: {}", getCloudPlatform(), sql);

    List<RowResult> tableResult =
        azureSynapsePdao.query(
            sql,
            (rs, rowNum) ->
                new AzureRowResult(
                    queryRequest.columnHeaderSchema().getColumnSchemas().stream()
                        .map(ColumnSchema::getColumnName)
                        .collect(Collectors.toMap(Function.identity(), name -> getField(name, rs))),
                    queryRequest.columnHeaderSchema()));

    return new QueryResult(tableResult, queryRequest.columnHeaderSchema());
  }

  @Override
  public void createTableFromQuery(TableId destinationTable, String sql, boolean isDryRun) {
    throw new NotImplementedException();
  }

  @Override
  public Collection<RowResult> readTableRows(Query query) {
    // FIXME: convert RowResult into azure row result, or create a generic row result type for query
    // response.
    if (true) {
      throw new NotImplementedException();
    }
    return azureSynapsePdao.query(
        renderSQL(query),
        (rs, rowNum) -> new AzureRowResult(Map.of(), new ColumnHeaderSchema(List.of())));
  }

  public DatasetTable getSchema(UUID datasetId, String tableName) {
    return datasetService.retrieve(datasetId).getTableByName(tableName).orElseThrow();
  }

  @Override
  public CloudPlatform getCloudPlatform() {
    return CloudPlatform.AZURE;
  }
}
