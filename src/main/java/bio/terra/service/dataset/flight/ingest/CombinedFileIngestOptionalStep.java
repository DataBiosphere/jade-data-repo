package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import java.util.Objects;

public record CombinedFileIngestOptionalStep(Step step) implements OptionalStep {
  @Override
  public boolean isEnabled(FlightContext context) {
    return IngestUtils.isCombinedFileIngest(context);
  }

  @Override
  public String getSkipReason() {
    return "there are no files to ingest";
  }

  @Override
  public String getRunReason(FlightContext context) {
    long numFiles =
        Objects.requireNonNullElse(
            context.getWorkingMap().get(IngestMapKeys.NUM_BULK_LOAD_FILE_MODELS, Long.class), 0L);
    return String.format("%s bulk load file(s) found to ingest", numFiles);
  }
}
