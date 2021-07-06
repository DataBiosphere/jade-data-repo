package bio.terra.service.dataset.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatasetGetOrCreateContainerStep implements Step {
  private static Logger logger =
      LoggerFactory.getLogger(CreateDatasetGetOrCreateContainerStep.class);
  private final ResourceService resourceService;
  private final DatasetRequestModel datasetRequestModel;
  private final AzureContainerPdao azureContainerPdao;
  private final AzureStorageAccountResource.ContainerType containerType;

  public CreateDatasetGetOrCreateContainerStep(
      ResourceService resourceService,
      DatasetRequestModel datasetRequestModel,
      AzureContainerPdao azureContainerPdao,
      AzureStorageAccountResource.ContainerType containerType) {
    this.resourceService = resourceService;
    this.datasetRequestModel = datasetRequestModel;
    this.azureContainerPdao = azureContainerPdao;
    this.containerType = containerType;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    logger.info("Creating a {} container for an Azure backed dataset", containerType);
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    AzureStorageAccountResource storageAccount =
        resourceService.getOrCreateStorageAccount(
            DatasetJsonConversion.datasetRequestToDataset(datasetRequestModel),
            profileModel,
            context.getFlightId());

    azureContainerPdao.getOrCreateContainer(profileModel, storageAccount, containerType);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Leaving artifacts on undo
    return StepResult.getStepResultSuccess();
  }
}
