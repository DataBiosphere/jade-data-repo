package bio.terra.service.common;

import static bio.terra.service.common.CommonMapKeys.SHOULD_PERFORM_CONTAINER_ROLLBACK;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.azure.storage.blob.BlobContainerClient;

/**
 * Extend this class to ensure that a created storage container gets created in a way that can be
 * cleaned up if the flight fails
 */
public abstract class CreateAzureContainerStep implements Step {
  protected final ResourceService resourceService;
  protected final AzureContainerPdao azureContainerPdao;

  public CreateAzureContainerStep(
      ResourceService resourceService, AzureContainerPdao azureContainerPdao) {
    this.resourceService = resourceService;
    this.azureContainerPdao = azureContainerPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    BillingProfileModel profileModel = getProfileModel(context);

    // Retrieve the internal object that represents the storage account
    AzureStorageAccountResource storageAccount =
        getAzureStorageAccountResource(context, profileModel);

    // Create the storage container if needed.  Note creating directly with the pdao since there
    // is no new metadata being recorded with this operation.
    BlobContainerClient container = azureContainerPdao.getContainer(profileModel, storageAccount);
    if (!container.exists()) {
      container.create();
      context.getWorkingMap().put(SHOULD_PERFORM_CONTAINER_ROLLBACK, true);
    } else {
      context.getWorkingMap().put(SHOULD_PERFORM_CONTAINER_ROLLBACK, false);
    }

    // Store the storage account in the working map.  This will be used in case of needing to undo
    context.getWorkingMap().put(getStorageAccountContextKey(), storageAccount);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel = getProfileModel(context);
    AzureStorageAccountResource datasetAzureStorageAccountResource =
        workingMap.get(getStorageAccountContextKey(), AzureStorageAccountResource.class);

    // If the container was created, delete it
    if (workingMap.get(SHOULD_PERFORM_CONTAINER_ROLLBACK, Boolean.class)) {
      resourceService.deleteStorageContainer(
          datasetAzureStorageAccountResource.getResourceId(),
          profileModel.getId(),
          context.getFlightId());
    }

    return StepResult.getStepResultSuccess();
  }

  /** Implement this method to return the billing profile model from the flight's context */
  protected BillingProfileModel getProfileModel(FlightContext context) {
    return context.getWorkingMap().get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
  }

  /**
   * Implement this method to return the key that will store the storage account metadata in the
   * flight's context
   */
  protected abstract String getStorageAccountContextKey();

  /** Implement this method to return the AzureStorageAccountResource from the flight's context */
  protected abstract AzureStorageAccountResource getAzureStorageAccountResource(
      FlightContext context, BillingProfileModel profileModel) throws InterruptedException;
}
