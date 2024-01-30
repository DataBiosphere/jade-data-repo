package bio.terra.common;

import bio.terra.buffer.model.ResourceInfo;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.exception.BufferServiceAPIException;
import bio.terra.service.resourcemanagement.exception.BufferServiceAuthorizationException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

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
    try {
      ResourceInfo resource = bufferService.handoutResource(enableSecureMonitoring);
      String projectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
      logger.info("Retrieved project from RBS with ID: {}", projectId);

      workingMap.put(ProjectCreatingFlightKeys.GOOGLE_PROJECT_ID, projectId);
      return StepResult.getStepResultSuccess();
    } catch (BufferServiceAPIException e) {
      // The NOT_FOUND status code indicates that Buffer Service is still creating a project and we
      // must retry. Retrying TOO_MANY_REQUESTS gives the service time to recover from load.
      // Add retry for internal server errors to help with test flakiness
      if (e.getStatusCode() == HttpStatus.NOT_FOUND
          || e.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS
          || e.getStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
      }
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    } catch (BufferServiceAuthorizationException e) {
      // If authorization fails, there is no recovering
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    } catch (GoogleResourceException e) {
      // thrown on error when refoldering project
      // We can consider retrying this if needed
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }
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
