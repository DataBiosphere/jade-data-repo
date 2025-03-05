package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;

public record SetAuthBqJobUserStep(
    ResourceService resourceService, String custodianEmail, boolean inheritSteward)
    implements Step {

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    FlightMap workingMap = flightContext.getWorkingMap();
    List<String> projectIds =
        workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_GOOGLE_PROJECT_IDS, new TypeReference<>() {});
    for (var projectId : projectIds) {
      if (inheritSteward) {
        resourceService.grantPoliciesBqJobUser(projectId, List.of(custodianEmail));
      } else {
        resourceService.revokePoliciesBqJobUser(projectId, List.of(custodianEmail));
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    FlightMap workingMap = flightContext.getWorkingMap();
    List<String> projectIds =
        workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_GOOGLE_PROJECT_IDS, new TypeReference<>() {});
    for (var projectId : projectIds) {
      if (!inheritSteward) {
        resourceService.grantPoliciesBqJobUser(projectId, List.of(custodianEmail));
      } else {
        resourceService.revokePoliciesBqJobUser(projectId, List.of(custodianEmail));
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
