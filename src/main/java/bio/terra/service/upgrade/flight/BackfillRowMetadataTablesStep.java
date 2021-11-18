package bio.terra.service.upgrade.flight;

import bio.terra.model.UpgradeModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackfillRowMetadataTablesStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(BackfillRowMetadataTablesStep.class);
  private UpgradeModel request;

  BackfillRowMetadataTablesStep(UpgradeModel request) {
    this.request = request;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    logger.info("HELLO FROM BackfillRowMetadataTablesStep");
    // enumerate datasets
    // for each dataset:
    // connect to big query for dataset's data project
    // for each table:
    // retrieve the row metadata table name from the database
    // check if row metadata table already exists in big query
    // if doesn't exist, create row metadata table
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
