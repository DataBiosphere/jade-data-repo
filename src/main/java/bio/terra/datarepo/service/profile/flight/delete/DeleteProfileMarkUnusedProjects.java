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

public class DeleteProfileMarkUnusedProjects implements Step {

  private final ResourceService resourceService;
  private final UUID profileId;

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteProfileMarkUnusedProjects.class);

  public DeleteProfileMarkUnusedProjects(ResourceService resourceService, UUID profileId) {
    this.resourceService = resourceService;
    this.profileId = profileId;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    List<UUID> projectIdList = resourceService.markUnusedProjectsForDelete(profileId);
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(ProfileMapKeys.PROFILE_PROJECT_ID_LIST, projectIdList);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
