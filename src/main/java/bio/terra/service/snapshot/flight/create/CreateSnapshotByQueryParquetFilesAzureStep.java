package bio.terra.service.snapshot.flight.create;

import bio.terra.common.exception.PdaoException;
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
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public class CreateSnapshotByQueryParquetFilesAzureStep extends CreateSnapshotParquetFilesAzureStep
    implements CreateSnapshotPrimaryDataQueryInterface {

  private final SnapshotDao snapshotDao;
  private final DatasetService datasetService;
  private final AuthenticatedUserRequest userRequest;
  private String sourceDatasetDataSourceName;
  private String targetDataSourceName;

  public CreateSnapshotByQueryParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      DatasetService datasetService,
      AuthenticatedUserRequest userRequest) {
    super(azureSynapsePdao, snapshotService);
    this.snapshotDao = snapshotDao;
    this.datasetService = datasetService;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    SnapshotRequestModel snapshotReq = getByQueryRequestModel(context);
    sourceDatasetDataSourceName = IngestUtils.getSourceDatasetDataSourceName(context.getFlightId());
    targetDataSourceName = IngestUtils.getTargetDataSourceName(context.getFlightId());
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    return prepareQueryAndCreateSnapshot(context, snapshot, snapshotReq, datasetService);
  }

  @Override
  public StepResult createSnapshotPrimaryData(
      FlightContext context,
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore) {
    FlightMap workingMap = context.getWorkingMap();
    SnapshotRequestModel snapshotReq = getByQueryRequestModel(context);
    try {
      Map<String, Long> tableRowCounts =
          azureSynapsePdao.createSnapshotParquetFilesByQuery(
              assetSpecification,
              snapshot.getId(),
              sourceDatasetDataSourceName,
              targetDataSourceName,
              sqlQuery,
              snapshotReq.isGlobalFileIds(),
              snapshot.getCompactIdPrefix());
      workingMap.put(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, tableRowCounts);
    } catch (SQLException | PdaoException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    return StepResult.getStepResultSuccess();
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
