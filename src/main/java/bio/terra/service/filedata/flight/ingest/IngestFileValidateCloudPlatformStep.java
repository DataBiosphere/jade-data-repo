package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.NotImplementedException;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestFileValidateCloudPlatformStep implements Step {

  private static final Logger logger =
      LoggerFactory.getLogger(IngestFileValidateCloudPlatformStep.class);
  private final Dataset dataset;

  public IngestFileValidateCloudPlatformStep(Dataset dataset) {
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    var profileModel = dataset.getDatasetSummary().getDefaultBillingProfile();
    var profileCloudPlatform = profileModel.getCloudPlatform();
    var profileWrapper = CloudPlatformWrapper.of(profileCloudPlatform);

    if (profileWrapper.isAzure()) {
      logger.warn(
          "Attempted to ingest file into Azure. Tenant: {}, Subscription: {}, Resource Group: {}",
          profileModel.getTenantId(),
          profileModel.getSubscriptionId(),
          profileModel.getResourceGroupName());
      NotImplementedException ex =
          new NotImplementedException("Can't ingest files into an Azure dataset yet.");
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    CloudPlatform fileStorageCloudPlatform = dataset.getDatasetSummary().getStorageCloudPlatform();
    if (!profileWrapper.is(fileStorageCloudPlatform)) {
      logger.warn(
          "The file storage cloud platform of this dataset ({}) does not match the billing cloud platform ({})",
          fileStorageCloudPlatform,
          profileCloudPlatform);
      IngestFailureException ex =
          new IngestFailureException(
              String.format(
                  "Can't ingest files into a %s storage using %s billing profile.",
                  fileStorageCloudPlatform, profileCloudPlatform));
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // This step has no side effects
    return StepResult.getStepResultSuccess();
  }
}
