package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.Query;
import bio.terra.grammar.azure.SynapseVisitor;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.stairway.FlightContext;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CreateSnapshotPrimaryDataQueryAzureStep extends CreateSnapshotParquetFilesAzureStep
    implements CreateSnapshotPrimaryDataQueryStep {

  private final AzureSynapsePdao azureSynapsePdao;
  private final SnapshotService snapshotService;
  private final SnapshotDao snapshotDao;
  private final SnapshotRequestModel snapshotReq;
  private final DatasetService datasetService;
  private final AuthenticatedUserRequest userRequest;
  private String sourceDatasetDataSourceName;
  private String targetDataSourceName;

  public CreateSnapshotPrimaryDataQueryAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq,
      DatasetService datasetService,
      AuthenticatedUserRequest userRequest) {
    super(azureSynapsePdao, snapshotService, snapshotReq);
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
    this.snapshotDao = snapshotDao;
    this.datasetService = datasetService;
    this.userRequest = userRequest;
  }

  @Override
  public Map<String, Long> createSnapshotParquetFiles(
      List<SnapshotTable> tables, UUID snapshotId, FlightContext context)
      throws SQLException, InterruptedException {
    sourceDatasetDataSourceName = IngestUtils.getSourceDatasetDataSourceName(context.getFlightId());
    targetDataSourceName = IngestUtils.getTargetDataSourceName(context.getFlightId());
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    return prepareQueryAndCreateSnapshot(context, snapshot, snapshotReq, datasetService);
  }

  @Override
  public Map<String, Long> createSnapshotPrimaryData(
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore)
      throws SQLException {
    return azureSynapsePdao.createSnapshotParquetFilesByQuery(
        assetSpecification,
        snapshot.getId(),
        sourceDatasetDataSourceName,
        targetDataSourceName,
        sqlQuery,
        snapshotReq.isGlobalFileIds());
  }

  @Override
  public String translateQuery(Query query, Dataset dataset) {
    DatasetModel datasetModel = datasetService.retrieveModel(dataset, userRequest);
    Map<String, DatasetModel> datasetMap =
        Collections.singletonMap(dataset.getName(), datasetModel);
    SynapseVisitor synapseVisitor = new SynapseVisitor(datasetMap, sourceDatasetDataSourceName);
    return query.translateSql(synapseVisitor);
  }
}
