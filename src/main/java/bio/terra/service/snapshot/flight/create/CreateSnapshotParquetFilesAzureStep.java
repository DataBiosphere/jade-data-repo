package bio.terra.service.snapshot.flight.create;

import bio.terra.service.dataset.Dataset;
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

  public CreateSnapshotParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao, DatasetService datasetService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    List<DatasetTable> tables = dataset.getTables();

    try {
      azureSynapsePdao.createSnapshotParquetFiles(
          tables,
          snapshotId,
          IngestUtils.getIngestRequestDataSourceName(context.getFlightId()),
          IngestUtils.getTargetDataSourceName(context.getFlightId()),
          context.getFlightId());

    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    azureSynapsePdao.dropTables(
        Arrays.asList(IngestUtils.getIngestRequestDataSourceName(context.getFlightId())));
    return StepResult.getStepResultSuccess();
  }
}
