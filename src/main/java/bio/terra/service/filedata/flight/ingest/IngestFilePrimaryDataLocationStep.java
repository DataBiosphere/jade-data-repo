package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestFilePrimaryDataLocationStep implements Step {
  private final Logger logger = LoggerFactory.getLogger(IngestFilePrimaryDataLocationStep.class);
  private final ResourceService resourceService;
  private final Dataset dataset;

  public IngestFilePrimaryDataLocationStep(ResourceService resourceService, Dataset dataset) {
    this.resourceService = resourceService;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    if (IngestUtils.noFilesToIngest(context)) {
      return StepResult.getStepResultSuccess();
    }

    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      // Retrieve the already authorized billing profile from the working map and retrieve
      // or create a bucket in the context of that profile and the dataset.
      GoogleProjectResource googleProjectResource =
          workingMap.get(FileMapKeys.PROJECT_RESOURCE, GoogleProjectResource.class);

      try {
        GoogleBucketResource bucketForFile =
            resourceService.getOrCreateBucketForFile(
                dataset, googleProjectResource, context.getFlightId());
        workingMap.put(FileMapKeys.BUCKET_INFO, bucketForFile);
      } catch (BucketLockException blEx) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, blEx);
      } catch (GoogleResourceNamingException ex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // There is not much to undo here. It is possible that a bucket was created in the last step. We
    // could look to
    // see if there are no other files in the bucket and delete it here, but I think it is likely
    // the bucket will
    // be used again.
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    GoogleProjectResource googleProjectResource =
        workingMap.get(FileMapKeys.PROJECT_RESOURCE, GoogleProjectResource.class);

    try {
      resourceService.updateBucketMetadata(
          googleProjectResource.getGoogleProjectId(), billingProfile, context.getFlightId());
    } catch (GoogleResourceNamingException e) {
      logger.error(e.getMessage());
    }
    return StepResult.getStepResultSuccess();
  }
}
