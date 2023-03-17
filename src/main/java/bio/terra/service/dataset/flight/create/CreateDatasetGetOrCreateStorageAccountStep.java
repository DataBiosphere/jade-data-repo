package bio.terra.service.dataset.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatasetGetOrCreateStorageAccountStep implements Step {
  private static Logger logger =
      LoggerFactory.getLogger(CreateDatasetGetOrCreateStorageAccountStep.class);
  private final ResourceService resourceService;
  private final DatasetRequestModel datasetRequestModel;
  private final AzureBlobStorePdao azureBlobStorePdao;

  public CreateDatasetGetOrCreateStorageAccountStep(
      ResourceService resourceService,
      DatasetRequestModel datasetRequestModel,
      AzureBlobStorePdao azureBlobStorePdao) {
    this.resourceService = resourceService;
    this.datasetRequestModel = datasetRequestModel;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    logger.info("Creating a storage account for Azure backed dataset");
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);

    AzureStorageAccountResource storageAccount =
        resourceService.getOrCreateDatasetStorageAccount(
            DatasetJsonConversion.datasetRequestToDataset(datasetRequestModel, datasetId),
            profileModel,
            context.getFlightId());

    logger.info("Enabling Azure storage account logging");
    // Log files will reside in the storage account's $logs container
    azureBlobStorePdao.enableFileLogging(profileModel, storageAccount);

    workingMap.put(
        DatasetWorkingMapKeys.APPLICATION_DEPLOYMENT_RESOURCE_ID,
        storageAccount.getApplicationResource().getId());
    workingMap.put(
        DatasetWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_ID, storageAccount.getResourceId());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Leaving artifacts on undo
    return StepResult.getStepResultSuccess();
  }
}
