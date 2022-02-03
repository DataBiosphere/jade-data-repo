package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public class NonCombinedFileIngestOptionalStep extends OptionalStep {
  public NonCombinedFileIngestOptionalStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    return !IngestUtils.isCombinedFileIngest(context);
  }

  @Override
  public String getRunReason(FlightContext context) {
    return "there are no bulk load files to ingest";
  }
}
