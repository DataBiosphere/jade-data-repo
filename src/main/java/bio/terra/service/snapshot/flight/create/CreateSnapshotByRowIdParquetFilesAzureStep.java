package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CreateSnapshotByRowIdParquetFilesAzureStep
    extends CreateSnapshotParquetFilesAzureStep {
  private final SnapshotRequestModel snapshotRequestModel;

  public CreateSnapshotByRowIdParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotRequestModel) {
    super(azureSynapsePdao, snapshotService);
    this.snapshotRequestModel = snapshotRequestModel;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    SnapshotRequestContentsModel contentsModel = snapshotRequestModel.getContents().get(0);
    SnapshotRequestRowIdModel rowIdModel = contentsModel.getRowIdSpec();
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    List<SnapshotTable> tables = snapshotService.retrieveTables(snapshotId);

    try {
      Map<String, Long> tableRowCounts =
          azureSynapsePdao.createSnapshotParquetFilesByRowId(
              tables,
              snapshotId,
              IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
              IngestUtils.getTargetDataSourceName(context.getFlightId()),
              rowIdModel,
              snapshotRequestModel.isGlobalFileIds());
      workingMap.put(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, tableRowCounts);
    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    return StepResult.getStepResultSuccess();
  }
}
