package bio.terra.service.snapshot.flight.create;

import bio.terra.common.exception.PdaoException;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class CreateSnapshotByAssetParquetFilesAzureStep
    extends CreateSnapshotParquetFilesAzureStep {
  private final SnapshotRequestModel snapshotReq;

  public CreateSnapshotByAssetParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq,
      UUID snapshotId) {
    super(azureSynapsePdao, snapshotService, snapshotId);
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
    SnapshotRequestAssetModel assetModel = contentsModel.getAssetSpec();

    Snapshot snapshot = snapshotService.retrieveByName(snapshotReq.getName());
    SnapshotSource source = snapshot.getFirstSnapshotSource();

    AssetSpecification assetSpec = source.getAssetSpecification();

    try {
      Map<String, Long> tableRowCounts =
          azureSynapsePdao.createSnapshotParquetFilesByAsset(
              assetSpec,
              snapshot.getId(),
              IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
              IngestUtils.getTargetDataSourceName(context.getFlightId()),
              assetModel,
              snapshotReq.isGlobalFileIds(),
              snapshotReq.getCompactIdPrefix());
      workingMap.put(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, tableRowCounts);
    } catch (SQLException | PdaoException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    return StepResult.getStepResultSuccess();
  }
}
