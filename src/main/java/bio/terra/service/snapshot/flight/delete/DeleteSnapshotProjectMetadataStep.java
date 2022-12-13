package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotProjectMetadataStep extends DefaultUndoStep {

  private final ResourceService resourceService;

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteSnapshotProjectMetadataStep.class);

  public DeleteSnapshotProjectMetadataStep(ResourceService resourceService) {
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> projectIds =
        workingMap.get(SnapshotWorkingMapKeys.PROJECTS_MARKED_FOR_DELETE, List.class);
    resourceService.deleteProjectMetadata(projectIds);

    return StepResult.getStepResultSuccess();
  }
}
