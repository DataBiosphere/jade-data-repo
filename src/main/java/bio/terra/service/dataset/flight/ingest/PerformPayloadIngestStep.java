package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public record PerformPayloadIngestStep(Step step) implements OptionalStep {
  @Override
  public boolean isEnabled(FlightContext context) {
    return IngestUtils.isIngestFromPayload(context.getInputParameters());
  }
}
