package bio.terra.service.dataset.flight.create;

import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The step is only meant to be invoked for GCP backed datasets. */
public class CreateDatasetRegisterIngestServiceAccountStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(CreateDatasetRegisterIngestServiceAccountStep.class);
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
    } catch (Exception e) {
      logger.warn(
          String.format(
              "Service account, %s, may not be ready to use yet. Retrying.", datasetServiceAccount),
          e);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // This will get undone when we delete the project
    return StepResult.getStepResultSuccess();
  }
}
