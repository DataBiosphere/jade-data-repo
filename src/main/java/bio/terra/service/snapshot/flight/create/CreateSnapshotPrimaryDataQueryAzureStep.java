package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.Query;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import bio.terra.stairway.FlightContext;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class CreateSnapshotPrimaryDataQueryAzureStep extends CreateSnapshotParquetFilesAzureStep {

  private final AzureSynapsePdao azureSynapsePdao;
  private final SnapshotService snapshotService;
  private final SnapshotDao snapshotDao;
  private final SnapshotRequestModel snapshotReq;
  private final DatasetService datasetService;
  private final AuthenticatedUserRequest userRequest;
  private AssetSpecification assetSpecification;

  public CreateSnapshotPrimaryDataQueryAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq,
      DatasetService datasetService,
      AuthenticatedUserRequest userRequest) {
    super(azureSynapsePdao, snapshotService);
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
    this.snapshotDao = snapshotDao;
    this.datasetService = datasetService;
    this.userRequest = userRequest;
  }

  @Override
  public Map<String, Long> createSnapshotParquetFiles(
      List<SnapshotTable> tables, UUID snapshotId, FlightContext context) throws SQLException {
    // -----   COPIED FROM GCP ----
    // TODO: this assumes single-dataset snapshots, will need to add a loop for multiple
    // (based on the validation flight step that already occurred.)
    /*
     * get dataset and assetName
     * get asset from dataset
     * which gives the root table
     * to use in conjunction with the filtered row ids to create this snapshot
     */
    // Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    SnapshotRequestQueryModel snapshotQuerySpec = snapshotReq.getContents().get(0).getQuerySpec();
    String snapshotAssetName = snapshotQuerySpec.getAssetName();

    String snapshotQuery = snapshotReq.getContents().get(0).getQuerySpec().getQuery();

    String sqlQuery = parseQuery(snapshotQuery, snapshotAssetName);

    // now using the query, get the rowIds
    // insert the rowIds into the snapshot row ids table and then kick off the rest of the
    // relationship walking
    // Instant createdAt = CommonFlightUtils.getCreatedAt(context);

    // ------- END OF COPIED FROM GCP ------

    // SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
    // SnapshotRequestAssetModel assetModel = contentsModel.getAssetSpec();

    // SnapshotSource source = snapshot.getFirstSnapshotSource();

    // AssetSpecification assetSpec = source.getAssetSpecification();

    return azureSynapsePdao.createSnapshotParquetFilesByQuery(
        assetSpecification,
        snapshotId,
        IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
        IngestUtils.getTargetDataSourceName(context.getFlightId()),
        sqlQuery);
  }

  public String parseQuery(String userProvidedQuery, String snapshotAssetName) {
    Query query = Query.parse(userProvidedQuery);
    List<String> datasetNames = query.getDatasetNames();
    // TODO this makes the assumption that there is only one dataset
    // (based on the validation flight step that already occurred.)
    // This will change when more than 1 dataset is allowed
    String datasetName = datasetNames.get(0);

    Dataset dataset = datasetService.retrieveByName(datasetName);
    DatasetModel datasetModel = datasetService.retrieveModel(dataset, userRequest);

    // get asset out of dataset
    Optional<AssetSpecification> assetSpecOp =
        dataset.getAssetSpecificationByName(snapshotAssetName);
    assetSpecification =
        assetSpecOp.orElseThrow(() -> new AssetNotFoundException("Expected asset specification"));

    Map<String, DatasetModel> datasetMap = Collections.singletonMap(datasetName, datasetModel);
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);

    String sqlQuery = query.translateSql(bqVisitor);

    // validate that the root table is actually a table being queried in the query -->
    // and the grammar only picks up tables names in the from clause (though there may be more than
    // one)
    List<String> tableNames = query.getTableNames();
    String rootTablename = assetSpecification.getRootTable().getTable().getName();
    if (!tableNames.contains(rootTablename)) {
      throw new InvalidQueryException(
          "The root table of the selected asset is not present in this query");
    }

    return sqlQuery;
  }
}
