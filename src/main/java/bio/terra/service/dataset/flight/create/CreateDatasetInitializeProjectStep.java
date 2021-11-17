package bio.terra.service.dataset.flight.create;

import bio.terra.app.model.GoogleRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

/** The step is only meant to be invoked for GCP backed datasets. */
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
    GoogleRegion region = GoogleRegion.fromValueWithDefault(datasetRequestModel.getRegion());

    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);

    // Since we find projects by their names, this is idempotent. If this step fails and is rerun,
    // Either the project record will have been created and we will find it, or we will create it.
    UUID projectResourceId;
    try {
      projectResourceId =
          resourceService.getOrCreateDatasetProject(
              profileModel, projectId, region, datasetRequestModel.getName(), datasetId);
    } catch (GoogleResourceException e) {
      if (e.getCause().getMessage().contains("500 Internal Server Error")) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
      } else {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
      }
    }

    workingMap.put(DatasetWorkingMapKeys.PROJECT_RESOURCE_ID, projectResourceId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // At this time we do not delete projects, so no undo
    return StepResult.getStepResultSuccess();
  }
}
