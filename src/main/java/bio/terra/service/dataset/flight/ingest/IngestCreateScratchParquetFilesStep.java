package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.storage.blob.BlobUrlParts;
import java.sql.SQLException;
import java.util.List;

public class IngestCreateScratchParquetFilesStep implements Step {

  private AzureSynapsePdao azureSynapsePdao;
  private DatasetService datasetService;

  public IngestCreateScratchParquetFilesStep(
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
    if (IngestUtils.isCombinedFileIngest(context)) {
      ingestBlob =
          BlobUrlParts.parse(workingMap.get(IngestMapKeys.INGEST_CONTROL_FILE_PATH, String.class));
    } else {
      ingestBlob = BlobUrlParts.parse(ingestRequestModel.getPath());
    }

    try {
      azureSynapsePdao.createScratchParquetFiles(
          ingestRequestModel.getFormat(),
          targetTable,
          ingestBlob.getBlobName(),
          parquetFilePath,
          IngestUtils.getDataSourceName(ContainerType.SCRATCH, context.getFlightId()),
          IngestUtils.getIngestRequestDataSourceName(context.getFlightId()),
          IngestUtils.getSynapseScratchTableName(context.getFlightId()),
          ingestRequestModel.getCsvSkipLeadingRows(),
          ingestRequestModel.getCsvFieldDelimiter(),
          ingestRequestModel.getCsvQuote());

    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    azureSynapsePdao.dropTables(
        List.of(IngestUtils.getSynapseScratchTableName(context.getFlightId())));
    return StepResult.getStepResultSuccess();
  }
}
