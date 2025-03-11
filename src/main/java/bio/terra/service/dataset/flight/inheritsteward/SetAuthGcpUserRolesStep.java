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
import java.util.Objects;

public record SetAuthGcpUserRolesStep(
    ResourceService resourceService, String custodianEmail, boolean inheritSteward)
    implements Step {

  private StepResult setAuth(FlightContext flightContext, boolean inheritSteward)
      throws InterruptedException {
    FlightMap workingMap = flightContext.getWorkingMap();
    List<String> projectIds =
        workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_GOOGLE_PROJECT_IDS, new TypeReference<>() {});
    for (var projectId : Objects.requireNonNull(projectIds)) {
      if (inheritSteward) {
        resourceService.assignRolesForSnapshot(projectId, List.of(custodianEmail));
      } else {
        resourceService.revokeRolesForSnapshot(projectId, List.of(custodianEmail));
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    return setAuth(flightContext, inheritSteward);
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return setAuth(flightContext, !inheritSteward);
  }
}
