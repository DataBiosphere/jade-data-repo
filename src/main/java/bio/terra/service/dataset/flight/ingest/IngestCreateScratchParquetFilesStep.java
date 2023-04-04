package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.FolderType;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.storage.blob.BlobUrlParts;
import java.sql.SQLException;
import java.util.List;

public class IngestCreateScratchParquetFilesStep implements Step {

  private final AzureSynapsePdao azureSynapsePdao;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final DatasetService datasetService;
  private final AuthenticatedUserRequest userRequest;

  public IngestCreateScratchParquetFilesStep(
      AzureSynapsePdao azureSynapsePdao,
      AzureBlobStorePdao azureBlobStorePdao,
      DatasetService datasetService,
      AuthenticatedUserRequest userRequest) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.datasetService = datasetService;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String parquetFilePath =
        FolderType.SCRATCH.getPath(workingMap.get(IngestMapKeys.PARQUET_FILE_PATH, String.class));
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
          IngestUtils.getTargetDataSourceName(context.getFlightId()),
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
    FlightMap workingMap = context.getWorkingMap();

    azureSynapsePdao.dropTables(
        List.of(IngestUtils.getSynapseScratchTableName(context.getFlightId())));

    AzureStorageAccountResource storageAccountResource =
        workingMap.get(
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    String scratchParquetFile =
        FolderType.SCRATCH.getPath(workingMap.get(IngestMapKeys.PARQUET_FILE_PATH, String.class));
    azureBlobStorePdao.deleteScratchParquet(
        scratchParquetFile, storageAccountResource, userRequest);

    return StepResult.getStepResultSuccess();
  }
}
