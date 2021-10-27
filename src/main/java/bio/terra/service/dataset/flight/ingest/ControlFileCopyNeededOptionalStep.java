package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;

public class ControlFileCopyNeededOptionalStep extends OptionalStep {
  public ControlFileCopyNeededOptionalStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    return context.getWorkingMap().get(IngestMapKeys.CONTROL_FILE_NEEDS_COPY, Boolean.class);
  }
}
