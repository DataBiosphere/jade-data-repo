package bio.terra.service.profile.flight.delete;

import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteProfileDeleteUnusedProjects implements Step {

  private final ResourceService resourceService;

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteProfileDeleteUnusedProjects.class);

  public DeleteProfileDeleteUnusedProjects(ResourceService resourceService) {
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> projectIdList = workingMap.get(ProfileMapKeys.PROFILE_PROJECT_ID_LIST, List.class);
    resourceService.deleteUnusedProjects(projectIdList);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
