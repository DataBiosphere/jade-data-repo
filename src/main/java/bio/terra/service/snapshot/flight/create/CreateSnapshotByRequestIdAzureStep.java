package bio.terra.service.snapshot.flight.create;

import bio.terra.common.exception.PdaoException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;

public class CreateSnapshotByRequestIdAzureStep extends CreateSnapshotParquetFilesAzureStep
    implements CreateSnapshotByRequestIdInterface, Step {
  private final SnapshotRequestModel snapshotReq;
  private final SnapshotBuilderService snapshotBuilderService;
  private final SnapshotRequestDao snapshotRequestDao;
  private final SnapshotDao snapshotDao;
  private final AuthenticatedUserRequest userReq;
  String sourceDatasetDataSourceName;
  String targetDataSourceName;

  public CreateSnapshotByRequestIdAzureStep(
      SnapshotRequestModel snapshotReq,
      SnapshotBuilderService snapshotBuilderService,
      SnapshotService snapshotService,
      SnapshotRequestDao snapshotRequestDao,
      SnapshotDao snapshotDao,
      AuthenticatedUserRequest userReq,
      AzureSynapsePdao azureSynapsePdao) {
    super(azureSynapsePdao, snapshotService);
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotReq = snapshotReq;
    this.snapshotBuilderService = snapshotBuilderService;
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotDao = snapshotDao;
    this.userReq = userReq;
  }

  @Override
  public StepResult createSnapshot(
      FlightContext context,
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore)
      throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
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
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    sourceDatasetDataSourceName = IngestUtils.getSourceDatasetDataSourceName(context.getFlightId());
    targetDataSourceName = IngestUtils.getTargetDataSourceName(context.getFlightId());
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    return prepareAndCreateSnapshot(
        context,
        snapshot,
        snapshotReq,
        snapshotBuilderService,
        snapshotRequestDao,
        snapshotDao,
        userReq);
  }
}
