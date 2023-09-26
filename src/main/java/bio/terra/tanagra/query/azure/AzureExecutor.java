package bio.terra.tanagra.query.azure;

import static bio.terra.service.filedata.azure.AzureSynapsePdao.getDataSourceName;

import bio.terra.common.Column;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.query.SqlPlatform;
import bio.terra.tanagra.underlay.datapointer.AzureDataset;
import com.google.cloud.bigquery.TableId;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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
    LOGGER.info("Running SQL against {}: {}", getSqlPlatform(), sql);

    List<RowResult> tableResult = List.of();
    /*
    can refactor getTableData() to do this

        azureSynapsePdao.query(
            sql,
            (rs, rowNum) ->
                new AzureRowResult(
                    queryRequest.columnHeaderSchema().getColumnSchemas().stream()
                        .map(ColumnSchema::getColumnName)
                        .collect(Collectors.toMap(Function.identity(), name -> getField(name, rs))),
                    queryRequest.columnHeaderSchema()));
     */

    return new QueryResult(tableResult, queryRequest.columnHeaderSchema());
  }

  @Override
  public void createTableFromQuery(TableId destinationTable, String sql, boolean isDryRun) {
    throw new NotImplementedException();
  }

  static class ResultSetRowResult implements RowResult {

    private final List<AzureCellValue> cells = new ArrayList<>();

    public ResultSetRowResult(ResultSet rs) {
      try {
        var metadata = rs.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
          cells.add(
              new AzureCellValue(
                  Optional.ofNullable(rs.getObject(i)),
                  new ColumnSchema(
                      metadata.getColumnName(i), toSqlDataType(metadata.getColumnType(i)))));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    private static CellValue.SQLDataType toSqlDataType(int columnType) {
      return switch (columnType) {
        case Types.INTEGER, Types.TINYINT, Types.BIGINT -> CellValue.SQLDataType.INT64;
        case Types.NUMERIC, Types.FLOAT, Types.DOUBLE -> CellValue.SQLDataType.FLOAT;
        case Types.CHAR, Types.VARCHAR -> CellValue.SQLDataType.STRING;
        case Types.BOOLEAN -> CellValue.SQLDataType.BOOLEAN;
        case Types.DATE -> CellValue.SQLDataType.DATE;
        default -> throw new RuntimeException("unexpected sql type " + columnType);
      };
    }

    @Override
    public CellValue get(int index) {
      return cells.get(index);
    }

    @Override
    public CellValue get(String columnName) {
      return cells.stream()
          .filter(cell -> cell.name().equals(columnName))
          .findFirst()
          .orElseThrow();
    }

    @Override
    public int size() {
      return cells.size();
    }
  }

  @Override
  public Collection<RowResult> readTableRows(Query query) {
    return azureSynapsePdao.query(renderSQL(query), (rs, rowNum) -> new ResultSetRowResult(rs));
  }

  public DatasetTable getSchema(UUID datasetId, String tableName) {
    var table = new DatasetTable().name(tableName);
    if (tableName.equals("drug_exposure")) {
      table.columns(
          List.of(new Column().name("drug_exposure_end_date").type(TableDataType.STRING)));
    }
    return table;
    //    return datasetService.retrieve(datasetId).getTableByName(tableName).orElseThrow();
  }

  @Override
  public SqlPlatform getSqlPlatform() {
    return SqlPlatform.SYNAPSE;
  }
}
