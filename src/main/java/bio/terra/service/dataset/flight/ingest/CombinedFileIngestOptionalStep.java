package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public class CombinedFileIngestOptionalStep extends OptionalStep {
  public CombinedFileIngestOptionalStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    return IngestUtils.isCombinedFileIngest(context);
  }
}
