package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotProjectMetadataStep implements Step {

  private final ResourceService resourceService;

  private static final Logger logger =
      LoggerFactory.getLogger(DeleteSnapshotProjectMetadataStep.class);

  public DeleteSnapshotProjectMetadataStep(ResourceService resourceService) {
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    UUID projectId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_PROJECT_ID, UUID.class);
    resourceService.deleteProjectMetadata(List.of(projectId));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
