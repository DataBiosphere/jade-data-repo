package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.DataLocationSelector;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

/** Requests a Google project from the Resource Buffer Service and puts it in the working map. */
public class IngestFileGetProjectStep implements Step {
  private final ResourceService resourceService;
  private final Dataset dataset;
  private final DataLocationSelector dataLocationSelector;

  public IngestFileGetProjectStep(
      ResourceService resourceService, Dataset dataset, DataLocationSelector dataLocationSelector) {
    this.resourceService = resourceService;
    this.dataset = dataset;
    this.dataLocationSelector = dataLocationSelector;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // Requests a google project from RBS and puts it in the working map
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    final GoogleProjectResource datasetProject =
        resourceService.getProjectResource(dataset.getProjectResourceId());
    String sourceDatasetGoogleProjectId = datasetProject.getGoogleProjectId();
    String projectId =
        dataLocationSelector.projectIdForFile(
            dataset, sourceDatasetGoogleProjectId, billingProfile);
    workingMap.put(FileMapKeys.GOOGLE_PROJECT_ID, projectId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
