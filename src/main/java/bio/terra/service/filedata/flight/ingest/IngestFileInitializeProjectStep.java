package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public record IngestFileInitializeProjectStep(ResourceService resourceService, Dataset dataset)
    implements DefaultUndoStep {
  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      // Retrieve the already authorized billing profile from the working map and retrieve
      // or create a bucket in the context of that profile and the dataset.
      BillingProfileModel billingProfile =
          workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
      String projectId = workingMap.get(FileMapKeys.GOOGLE_PROJECT_ID, String.class);
      try {
        GoogleProjectResource projectResource =
            resourceService.initializeProjectForBucket(dataset, billingProfile, projectId);
        workingMap.put(FileMapKeys.PROJECT_RESOURCE, projectResource);
      } catch (BucketLockException blEx) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, blEx);
      } catch (GoogleResourceException ex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
