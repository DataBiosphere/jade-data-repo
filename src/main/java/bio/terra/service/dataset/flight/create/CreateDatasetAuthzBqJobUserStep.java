package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.iam.IamRole;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.Map;
import java.util.UUID;

public class CreateDatasetAuthzBqJobUserStep implements Step {
  private final DatasetService datasetService;
  private final ResourceService resourceService;

  public CreateDatasetAuthzBqJobUserStep(
      DatasetService datasetService, ResourceService resourceService) {
    this.datasetService = datasetService;
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    Map<IamRole, String> policies = workingMap.get(DatasetWorkingMapKeys.POLICY_EMAILS, Map.class);
    Dataset dataset = datasetService.retrieve(datasetId);
    resourceService.grantPoliciesBqJobUser(
        dataset.getProjectResource().getGoogleProjectId(), policies.values());

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // TODO: Remove policies from the project DR-1093
    return StepResult.getStepResultSuccess();
  }
}
