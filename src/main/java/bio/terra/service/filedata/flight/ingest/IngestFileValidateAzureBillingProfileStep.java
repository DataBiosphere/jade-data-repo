package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestFileValidateAzureBillingProfileStep implements Step {

  private static final Logger logger =
      LoggerFactory.getLogger(IngestFileValidateAzureBillingProfileStep.class);
  private final UUID profileId;
  private final Dataset dataset;

  public IngestFileValidateAzureBillingProfileStep(UUID profileId, Dataset dataset) {
    this.profileId = profileId;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    var defaultBillingProfileId = dataset.getDefaultProfileId();
    if (!defaultBillingProfileId.equals(profileId)) {
      logger.warn(
          "The billing profile in the ingest request ({}) does not match the default billing profile ({})",
          profileId,
          defaultBillingProfileId);
      IngestFailureException ex =
          new IngestFailureException(
              String.format(
                  "Can't ingest files using a different billing profile %s than the Azure dataset %s.",
                  profileId, defaultBillingProfileId));
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
