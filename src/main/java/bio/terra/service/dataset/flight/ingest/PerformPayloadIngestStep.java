package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public class PerformPayloadIngestStep extends OptionalStep {
  public PerformPayloadIngestStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    return IngestUtils.isIngestFromPayload(context.getInputParameters());
  }
}
