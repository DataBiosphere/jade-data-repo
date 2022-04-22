package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.sql.SQLException;
import java.util.List;

public class IngestValidateScratchTableStep implements Step {

  private AzureSynapsePdao azureSynapsePdao;
  private DatasetService datasetService;

  public IngestValidateScratchTableStep(
      AzureSynapsePdao azureSynapsePdao, DatasetService datasetService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);

    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    int failCount;
    try {
      failCount =
          azureSynapsePdao.validateScratchParquetFiles(
              targetTable, IngestUtils.getSynapseScratchTableName(context.getFlightId()));

    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    if (failCount > ingestRequestModel.getMaxBadRecords()) {
      throw new IngestFailureException(
          String.format("Failed to load data into dataset %s", dataset.getId()),
          List.of(
              String.format(
                  "%d records failed to ingest, which is equal to or more than the %d allowed failed records",
                  failCount, ingestRequestModel.getMaxBadRecords()),
              "Check that all records have data for columns marked as required in the dataset schema."));
    }

    workingMap.put(IngestMapKeys.AZURE_ROWS_FAILED_VALIDATION, failCount);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
