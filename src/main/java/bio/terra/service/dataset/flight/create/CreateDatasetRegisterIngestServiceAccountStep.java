package bio.terra.service.dataset.flight.create;

import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

/** The step is only meant to be invoked for GCP backed datasets. */
public class CreateDatasetRegisterIngestServiceAccountStep implements Step {

  private final IamService iamService;

  public CreateDatasetRegisterIngestServiceAccountStep(IamService iamService) {
    this.iamService = iamService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String datasetServiceAccount =
        workingMap.get(DatasetWorkingMapKeys.SERVICE_ACCOUNT_EMAIL, String.class);

    try {
      iamService.registerUser(datasetServiceAccount);
    } catch (GoogleResourceException e) {
      if (e.getCause().getMessage().contains("Could not generate Google access token")) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
      } else {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
      }
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // This will get undone when we delete the project
    return StepResult.getStepResultSuccess();
  }
}
