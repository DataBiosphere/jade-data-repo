package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.FolderType;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.sql.SQLException;
import java.util.List;

public class IngestCreateParquetFilesStep implements Step {

  private AzureSynapsePdao azureSynapsePdao;
  private DatasetService datasetService;

  public IngestCreateParquetFilesStep(
      AzureSynapsePdao azureSynapsePdao, DatasetService datasetService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    String parquetFilePath =
        FolderType.METADATA.getPath(workingMap.get(IngestMapKeys.PARQUET_FILE_PATH, String.class));
    DatasetTable datasetTable = IngestUtils.getDatasetTable(context, dataset);

    int failedRowCount = workingMap.get(IngestMapKeys.AZURE_ROWS_FAILED_VALIDATION, Integer.class);

    long updateCount;
    try {
      updateCount =
          azureSynapsePdao.createFinalParquetFiles(
              IngestUtils.getSynapseIngestTableName(context.getFlightId()),
              parquetFilePath,
              IngestUtils.getTargetDataSourceName(context.getFlightId()),
              IngestUtils.getSynapseScratchTableName(context.getFlightId()),
              datasetTable);

    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);

    Long failedCombinedRowCount = 0L;
    if (workingMap.containsKey(IngestMapKeys.COMBINED_FAILED_ROW_COUNT)) {
      failedCombinedRowCount = workingMap.get(IngestMapKeys.COMBINED_FAILED_ROW_COUNT, Long.class);
    }

    IngestResponseModel ingestResponse =
        new IngestResponseModel()
            .dataset(dataset.getName())
            .datasetId(dataset.getId())
            .table(ingestRequest.getTable())
            .path(ingestRequest.getPath())
            .loadTag(ingestRequest.getLoadTag())
            .badRowCount(failedCombinedRowCount + failedRowCount)
            .rowCount(updateCount);

    if (IngestUtils.isCombinedFileIngest(context)) {
      BulkLoadArrayResultModel fileLoadResults =
          workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class);
      ingestResponse.loadResult(fileLoadResults);
    }

    // If loading from a payload, there is no path to report to the user
    if (IngestUtils.isIngestFromPayload(context.getInputParameters())) {
      ingestResponse.setPath(null);
    }

    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), ingestResponse);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    azureSynapsePdao.dropTables(
        List.of(IngestUtils.getSynapseIngestTableName(context.getFlightId())));
    return StepResult.getStepResultSuccess();
  }
}
