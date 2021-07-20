package bio.terra.service.dataset.flight.create;

import bio.terra.app.model.GoogleRegion;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class CreateDatasetInitializeProjectStep implements Step {
  private final ResourceService resourceService;
  private final DatasetRequestModel datasetRequestModel;

  public CreateDatasetInitializeProjectStep(
      ResourceService resourceService, DatasetRequestModel datasetRequestModel) {
    this.resourceService = resourceService;
    this.datasetRequestModel = datasetRequestModel;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String projectId = workingMap.get(DatasetWorkingMapKeys.GOOGLE_PROJECT_ID, String.class);
    CloudPlatformWrapper platformWrapper =
        CloudPlatformWrapper.of(datasetRequestModel.getCloudPlatform());
    GoogleRegion region = platformWrapper
        .getGoogleRegionFromDatasetRequestModel(datasetRequestModel);
    Boolean isAzure = platformWrapper.isAzure();

    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);

    // Since we find projects by their names, this is idempotent. If this step fails and is rerun,
    // Either the project record will have been created and we will find it, or we will create it.
    UUID projectResourceId =
        resourceService.getOrCreateDatasetProject(
            profileModel, projectId, region, datasetRequestModel.getName(), datasetId, isAzure);
    workingMap.put(DatasetWorkingMapKeys.PROJECT_RESOURCE_ID, projectResourceId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // At this time we do not delete projects, so no undo
    return StepResult.getStepResultSuccess();
  }
}
