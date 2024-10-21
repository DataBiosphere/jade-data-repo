package bio.terra.service.dataset.flight.create;

import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

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

    iamService.registerUser(datasetServiceAccount);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // This will get undone when we delete the project
    return StepResult.getStepResultSuccess();
  }
}
