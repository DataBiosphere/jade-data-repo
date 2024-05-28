package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
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

public class CreateSnapshotByFullViewParquetFilesAzureStep
    extends CreateSnapshotParquetFilesAzureStep {

  private final SnapshotRequestModel snapshotReq;

  public CreateSnapshotByFullViewParquetFilesAzureStep(
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

    List<SnapshotTable> tables = snapshotService.retrieveTables(snapshotId);
    try {
      Map<String, Long> tableRowCounts =
          azureSynapsePdao.createSnapshotParquetFiles(
              tables,
              snapshotId,
              IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
              IngestUtils.getTargetDataSourceName(context.getFlightId()),
              snapshotReq.isGlobalFileIds(),
              snapshotReq.getCompactIdPrefix());
      workingMap.put(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, tableRowCounts);
    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    return StepResult.getStepResultSuccess();
  }
}
