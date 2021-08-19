package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.function.Predicate;

/** Requests a Google project from the Resource Buffer Service and puts it in the working map. */
public class IngestFileGetProjectStep extends SkippableStep {
  private final Dataset dataset;
  private final GoogleProjectService googleProjectService;

  public IngestFileGetProjectStep(
      Dataset dataset,
      GoogleProjectService googleProjectService,
      Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.dataset = dataset;
    this.googleProjectService = googleProjectService;
  }

  public IngestFileGetProjectStep(Dataset dataset, GoogleProjectService googleProjectService) {
    this(dataset, googleProjectService, SkippableStep::neverSkip);
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) {
    // Requests a google project from RBS and puts it in the working map
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String projectId = googleProjectService.projectIdForFile(dataset, billingProfile);
    workingMap.put(FileMapKeys.GOOGLE_PROJECT_ID, projectId);
    return StepResult.getStepResultSuccess();
  }
}
