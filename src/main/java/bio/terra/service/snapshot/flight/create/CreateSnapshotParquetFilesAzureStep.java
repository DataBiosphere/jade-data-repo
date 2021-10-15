package bio.terra.service.snapshot.flight.create;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class CreateSnapshotParquetFilesAzureStep implements Step {

  private AzureSynapsePdao azureSynapsePdao;
  private DatasetService datasetService;
  private String datasetName;

  public CreateSnapshotParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao, DatasetService datasetService, String datasetName) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
    this.datasetName = datasetName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

    List<DatasetTable> tables = datasetService.retrieveByName(datasetName).getTables();

    try {
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
          null);

    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    azureSynapsePdao.dropTables(
        Arrays.asList(IngestUtils.getSourceDatasetDataSourceName(context.getFlightId())));
    return StepResult.getStepResultSuccess();
  }
}
