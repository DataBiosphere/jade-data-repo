package bio.terra.datarepo.service.filedata.flight.ingest;

import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.service.dataset.Dataset;
import bio.terra.datarepo.service.filedata.flight.FileMapKeys;
import bio.terra.datarepo.service.profile.flight.ProfileMapKeys;
import bio.terra.datarepo.service.resourcemanagement.ResourceService;
import bio.terra.datarepo.service.resourcemanagement.exception.BucketLockException;
import bio.terra.datarepo.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.datarepo.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.datarepo.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestFileGetOrCreateProject implements Step {
  private final Logger logger = LoggerFactory.getLogger(IngestFileGetOrCreateProject.class);
  private final ResourceService resourceService;
  private final Dataset dataset;

  public IngestFileGetOrCreateProject(ResourceService resourceService, Dataset dataset) {
    this.resourceService = resourceService;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      // Retrieve the already authorized billing profile from the working map and retrieve
      // or create a bucket in the context of that profile and the dataset.
      BillingProfileModel billingProfile =
          workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

      try {
        GoogleProjectResource projectResource =
            resourceService.getOrCreateProjectForBucket(dataset, billingProfile);
        workingMap.put(FileMapKeys.PROJECT_RESOURCE, projectResource);
      } catch (BucketLockException blEx) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, blEx);
      } catch (GoogleResourceException | GoogleResourceNamingException ex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
      }
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // At this time we do not delete projects, so no undo
    return StepResult.getStepResultSuccess();
  }
}
