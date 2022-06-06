package bio.terra.common;

import bio.terra.buffer.model.ResourceInfo;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetResourceBufferProjectStep implements Step {
  private final Logger logger = LoggerFactory.getLogger(GetResourceBufferProjectStep.class);

  private final BufferService bufferService;
  private final GoogleResourceManagerService googleResourceManagerService;
  private final boolean enableSecureMonitoring;

  public GetResourceBufferProjectStep(
      BufferService bufferService,
      GoogleResourceManagerService googleResourceManagerService,
      boolean enableSecureMonitoring) {
    this.bufferService = bufferService;
    this.googleResourceManagerService = googleResourceManagerService;
    this.enableSecureMonitoring = enableSecureMonitoring;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    ResourceInfo resource = bufferService.handoutResource(enableSecureMonitoring);
    String projectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    logger.info("Retrieved project from RBS with ID: {}", projectId);

    workingMap.put(ProjectCreatingFlightKeys.GOOGLE_PROJECT_ID, projectId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    String projectId = workingMap.get(ProjectCreatingFlightKeys.GOOGLE_PROJECT_ID, String.class);
    if (projectId != null) {
      // No need to clean up metadata since this a creation undo step and the metadata could be
      // incomplete
      googleResourceManagerService.deleteProject(projectId);
    }
    return StepResult.getStepResultSuccess();
  }
}
