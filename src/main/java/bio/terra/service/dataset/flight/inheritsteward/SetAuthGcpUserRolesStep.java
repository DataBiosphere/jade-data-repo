package bio.terra.service.dataset.flight.inheritsteward;

import static bio.terra.service.resourcemanagement.ResourceService.BQ_JOB_USER_ROLE;
import static bio.terra.service.resourcemanagement.ResourceService.SERVICE_USAGE_CONSUMER_ROLE;

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

  public static final List<String> SNAPSHOT_GCP_IAM_ROLES =
      List.of(BQ_JOB_USER_ROLE, SERVICE_USAGE_CONSUMER_ROLE);

  private StepResult setAuth(FlightContext flightContext, boolean inheritSteward)
      throws InterruptedException {
    FlightMap workingMap = flightContext.getWorkingMap();
    List<String> projectIds =
        workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_GOOGLE_PROJECT_IDS, new TypeReference<>() {});
    for (var projectId : Objects.requireNonNull(projectIds)) {
      if (inheritSteward) {
        resourceService.grantPoliciesForRoles(
            projectId, List.of(custodianEmail), SNAPSHOT_GCP_IAM_ROLES);
      } else {
        resourceService.revokePoliciesForRoles(
            projectId, List.of(custodianEmail), SNAPSHOT_GCP_IAM_ROLES);
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
