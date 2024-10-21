package bio.terra.service.dataset.flight.create;

import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

/** The step is only meant to be invoked for GCP backed datasets. */
public class CreateDatasetCreateIngestServiceAccountStep implements Step {
  private final ResourceService resourceService;
  private final DatasetRequestModel datasetRequestModel;

  public CreateDatasetCreateIngestServiceAccountStep(
      ResourceService resourceService, DatasetRequestModel datasetRequestModel) {
    this.resourceService = resourceService;
    this.datasetRequestModel = datasetRequestModel;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String projectId = workingMap.get(DatasetWorkingMapKeys.GOOGLE_PROJECT_ID, String.class);

    try {
      String datasetServiceAccount =
          resourceService.createDatasetServiceAccount(projectId, datasetRequestModel.getName());
      workingMap.put(DatasetWorkingMapKeys.SERVICE_ACCOUNT_EMAIL, datasetServiceAccount);
    } catch (GoogleResourceException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // This will get undone when we delete the project
    return StepResult.getStepResultSuccess();
  }
}
