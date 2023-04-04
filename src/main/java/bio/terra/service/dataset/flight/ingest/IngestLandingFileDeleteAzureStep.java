package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobUrlParts;

public class IngestLandingFileDeleteAzureStep implements Step {

  private final boolean performInUndoPhase;
  private final AzureContainerPdao azureContainerPdao;

  public IngestLandingFileDeleteAzureStep(
      boolean performInUndoPhase, AzureContainerPdao azureContainerPdao) {
    this.performInUndoPhase = performInUndoPhase;
    this.azureContainerPdao = azureContainerPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    if (!performInUndoPhase) {
      return performDelete(context);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    if (performInUndoPhase) {
      return performDelete(context);
    }
    return StepResult.getStepResultSuccess();
  }

  private StepResult performDelete(FlightContext context)
      throws InterruptedException, RetryException {
    // Clean up the file where data was staged to ingest
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount =
        workingMap.get(
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    IngestRequestModel ingestRequestModel =
        context.getInputParameters().get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);
    String pathToLandingFile = ingestRequestModel.getPath();

    BlobContainerClient containerClient =
        azureContainerPdao.getOrCreateContainer(profile, storageAccount);

    String blobName = BlobUrlParts.parse(pathToLandingFile).getBlobName();
    containerClient.getBlobClient(blobName).delete();

    return StepResult.getStepResultSuccess();
  }
}
