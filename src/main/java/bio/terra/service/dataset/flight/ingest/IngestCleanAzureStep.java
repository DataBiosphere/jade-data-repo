package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.FolderType;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;

public class IngestCleanAzureStep implements Step {
  private final AzureSynapsePdao azureSynapsePdao;
  private final AzureBlobStorePdao azureBlobStorePdao;

  private final AuthenticatedUserRequest userRequest;

  public IngestCleanAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      AzureBlobStorePdao azureBlobStorePdao,
      AuthenticatedUserRequest userRequest) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    azureSynapsePdao.dropTables(
        List.of(
            IngestUtils.getSynapseScratchTableName(context.getFlightId()),
            IngestUtils.getSynapseIngestTableName(context.getFlightId())));
    azureSynapsePdao.dropDataSources(
        List.of(
            IngestUtils.getTargetDataSourceName(context.getFlightId()),
            IngestUtils.getIngestRequestDataSourceName(context.getFlightId())));
    azureSynapsePdao.dropScopedCredentials(
        List.of(
            IngestUtils.getTargetScopedCredentialName(context.getFlightId()),
            IngestUtils.getIngestRequestScopedCredentialName(context.getFlightId())));

    AzureStorageAccountResource storageAccountResource =
        workingMap.get(
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    String scratchParquetFile =
        FolderType.SCRATCH.getPath(workingMap.get(IngestMapKeys.PARQUET_FILE_PATH, String.class));
    azureBlobStorePdao.deleteScratchParquet(
        scratchParquetFile, storageAccountResource, userRequest);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
