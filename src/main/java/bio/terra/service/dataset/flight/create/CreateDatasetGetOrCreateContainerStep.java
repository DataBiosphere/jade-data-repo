package bio.terra.service.dataset.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatasetGetOrCreateContainerStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(CreateDatasetGetOrCreateContainerStep.class);
  private final ResourceService resourceService;
  private final DatasetRequestModel datasetRequestModel;
  private final AzureContainerPdao azureContainerPdao;

  public CreateDatasetGetOrCreateContainerStep(
      ResourceService resourceService,
      DatasetRequestModel datasetRequestModel,
      AzureContainerPdao azureContainerPdao) {
    this.resourceService = resourceService;
    this.datasetRequestModel = datasetRequestModel;
    this.azureContainerPdao = azureContainerPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    logger.info("Creating a container for an Azure backed dataset");
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);

    AzureStorageAccountResource storageAccount =
        resourceService.getOrCreateDatasetStorageAccount(
            DatasetJsonConversion.datasetRequestToDataset(datasetRequestModel, datasetId),
            profileModel,
            context.getFlightId());

    azureContainerPdao.getOrCreateContainer(profileModel, storageAccount);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Leaving artifacts on undo
    return StepResult.getStepResultSuccess();
  }
}
