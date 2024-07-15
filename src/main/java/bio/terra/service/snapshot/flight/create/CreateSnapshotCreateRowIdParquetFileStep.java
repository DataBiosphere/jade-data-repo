package bio.terra.service.snapshot.flight.create;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSnapshotCreateRowIdParquetFileStep implements Step {
  private Logger logger = LoggerFactory.getLogger(CreateSnapshotCreateRowIdParquetFileStep.class);

  private final AzureSynapsePdao azureSynapsePdao;
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public CreateSnapshotCreateRowIdParquetFileStep(
      AzureSynapsePdao azureSynapsePdao, SnapshotService snapshotService, UUID snapshotId) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    FlightMap workingMap = flightContext.getWorkingMap();
    Map<String, Long> tableRowCounts =
        workingMap.get(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, HashMap.class);
    List<SnapshotTable> tables = snapshotService.retrieveTables(snapshotId);
    try {
      azureSynapsePdao.createSnapshotRowIdsParquetFile(
          tables,
          snapshotId,
          IngestUtils.getTargetDataSourceName(flightContext.getFlightId()),
          tableRowCounts);
    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    azureSynapsePdao.dropTables(
        List.of(IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE)));
    return StepResult.getStepResultSuccess();
  }
}
