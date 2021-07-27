package bio.terra.common;

import bio.terra.buffer.model.ResourceInfo;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetResourceBufferProjectStep implements Step {
  private final Logger logger = LoggerFactory.getLogger(GetResourceBufferProjectStep.class);

  private final BufferService bufferService;

  public GetResourceBufferProjectStep(BufferService bufferService) {
    this.bufferService = bufferService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    ResourceInfo resource = bufferService.handoutResource();
    String projectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    logger.info("Retrieved project from RBS with ID: {}", projectId);

    workingMap.put(ProjectCreatingFlightKeys.GOOGLE_PROJECT_ID, projectId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
