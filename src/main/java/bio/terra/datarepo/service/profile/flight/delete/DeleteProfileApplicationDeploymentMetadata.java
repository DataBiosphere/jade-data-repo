package bio.terra.datarepo.service.profile.flight.delete;

import bio.terra.datarepo.service.profile.flight.ProfileMapKeys;
import bio.terra.datarepo.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteProfileApplicationDeploymentMetadata implements Step {

  private final ResourceService resourceService;

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteProfileApplicationDeploymentMetadata.class);

  public DeleteProfileApplicationDeploymentMetadata(ResourceService resourceService) {
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> appIdList =
        workingMap.get(ProfileMapKeys.PROFILE_APPLICATION_DEPLOYMENT_ID_LIST, List.class);
    resourceService.deleteDeployedApplicationMetadata(appIdList);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
