package bio.terra.tanagra.query.azure;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.RowResult;
import com.google.cloud.bigquery.TableId;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
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

  private final AzureSynapsePdao azureSynapsePdao;
  private final DatasetService datasetService;

  public AzureExecutor(AzureSynapsePdao azureSynapsePdao, DatasetService datasetService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
  }

  private static Optional<Object> getField(String name, ResultSet resultSet) {
    try {
      return Optional.ofNullable(resultSet.getObject(name));
    } catch (SQLException e) {
      return Optional.empty();
    }
  }

  @Override
  public QueryResult execute(QueryRequest queryRequest) {
    String sql = queryRequest.getSql();
    LOGGER.info("Running SQL against Azure: {}", sql);
    List<RowResult> tableResult =
        azureSynapsePdao.query(
            sql,
            (rs, rowNum) ->
                new AzureRowResult(
                    queryRequest.getColumnHeaderSchema().getColumnSchemas().stream()
                        .map(ColumnSchema::getColumnName)
                        .collect(Collectors.toMap(Function.identity(), name -> getField(name, rs))),
                    queryRequest.getColumnHeaderSchema()));

    return new QueryResult(tableResult, queryRequest.getColumnHeaderSchema());
  }

  @Override
  public void createTableFromQuery(TableId destinationTable, String sql, boolean isDryRun) {
    throw new NotImplementedException();
  }

  public DatasetTable getSchema(UUID datasetId, String tableName) {
    return datasetService.retrieve(datasetId).getTableByName(tableName).orElseThrow();
  }
}
