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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class CreateSnapshotParquetFilesAzureStep implements Step {

  private AzureSynapsePdao azureSynapsePdao;
  private SnapshotService snapshotService;

  public CreateSnapshotParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao, SnapshotService snapshotService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

    List<SnapshotTable> tables = snapshotService.retrieveTables(snapshotId);

    try {
      Map<String, Long> tableRowCounts =
          azureSynapsePdao.createSnapshotParquetFiles(
              tables,
              snapshotId,
              IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
              IngestUtils.getTargetDataSourceName(context.getFlightId()),
              null);

      azureSynapsePdao.createSnapshotRowIdsParquetFile(
          tables,
          snapshotId,
          IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
          IngestUtils.getTargetDataSourceName(context.getFlightId()),
          tableRowCounts,
          null);

      workingMap.put(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, tableRowCounts);

    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    List<SnapshotTable> tables = snapshotService.retrieveTables(snapshotId);

    azureSynapsePdao.dropTables(
        tables.stream()
            .map(table -> IngestUtils.formatSnapshotTableName(snapshotId, table.getName()))
            .collect(Collectors.toList()));
    azureSynapsePdao.dropTables(
        List.of(IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE)));
    return StepResult.getStepResultSuccess();
  }
}
