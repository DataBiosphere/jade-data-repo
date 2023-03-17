package bio.terra.service.dataset.flight.ingest.scratch;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import com.azure.storage.blob.BlobContainerClient;

public class CreateScratchFileForAzureStep extends DefaultUndoStep {

  private final AzureContainerPdao azureContainerPdao;

  public CreateScratchFileForAzureStep(AzureContainerPdao azureContainerPdao) {
    this.azureContainerPdao = azureContainerPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String tableName = context.getInputParameters().get(IngestMapKeys.TABLE_NAME, String.class);
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount =
        workingMap.get(
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    BlobContainerClient containerClient =
        azureContainerPdao.getOrCreateContainer(billingProfile, storageAccount);

    String path =
        containerClient
            .getBlobClient(context.getFlightId() + "/ingest-scratch/" + tableName + ".json")
            .getBlobUrl();

    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), path);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
