package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.storage.blob.BlobUrlParts;
import java.sql.SQLException;
import java.util.Arrays;

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
    IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String parquetFilePath = workingMap.get(IngestMapKeys.PARQUET_FILE_PATH, String.class);
    final BlobUrlParts ingestBlob;
    if (IngestUtils.noFilesToIngest.test(context)) {
      ingestBlob = BlobUrlParts.parse(ingestRequestModel.getPath());
    } else {
      ingestBlob =
          BlobUrlParts.parse(workingMap.get(IngestMapKeys.INGEST_SCRATCH_FILE_PATH, String.class));
    }

    long updateCount;
    try {
      updateCount =
          azureSynapsePdao.createParquetFiles(
              ingestRequestModel.getFormat(),
              targetTable,
              ingestBlob.getBlobName(),
              parquetFilePath,
              IngestUtils.getTargetDataSourceName(context.getFlightId()),
              IngestUtils.getIngestRequestDataSourceName(context.getFlightId()),
              IngestUtils.getSynapseTableName(context.getFlightId()),
              ingestRequestModel.getCsvSkipLeadingRows(),
              ingestRequestModel.getCsvFieldDelimiter(),
              ingestRequestModel.getCsvQuote());

    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);

    Long failedRowCount = 0L;
    if (workingMap.containsKey(IngestMapKeys.COMBINED_FAILED_ROW_COUNT)) {
      failedRowCount = workingMap.get(IngestMapKeys.COMBINED_FAILED_ROW_COUNT, Long.class);
    }

    IngestResponseModel ingestResponse =
        new IngestResponseModel()
            .dataset(dataset.getName())
            .datasetId(dataset.getId())
            .table(ingestRequest.getTable())
            .path(ingestRequest.getPath())
            .loadTag(ingestRequest.getLoadTag())
            .badRowCount(failedRowCount) // Azure only has failed rows if combined ingest does.
            .rowCount(updateCount);

    if (!IngestUtils.noFilesToIngest.test(context)) {
      BulkLoadArrayResultModel fileLoadResults =
          workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class);
      ingestResponse.loadResult(fileLoadResults);
    }

    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), ingestResponse);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    azureSynapsePdao.dropTables(
        Arrays.asList(IngestUtils.getIngestRequestDataSourceName(context.getFlightId())));
    return StepResult.getStepResultSuccess();
  }
}
