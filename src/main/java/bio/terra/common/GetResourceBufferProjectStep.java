package bio.terra.common;

import bio.terra.buffer.model.ResourceInfo;
import bio.terra.model.DatasetSecurityClassification;
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
  private final DatasetSecurityClassification securityClassification;

  public GetResourceBufferProjectStep(
      BufferService bufferService, DatasetSecurityClassification securityClassification) {
    this.bufferService = bufferService;
    this.securityClassification = securityClassification;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    ResourceInfo resource = bufferService.handoutResource(securityClassification);
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
