package bio.terra.service.dataset.flight.datadelete;

import bio.terra.common.FlightUtils;
import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import java.util.Set;

public class ControlFileCopyNeededOptionalStep extends OptionalStep {
  public ControlFileCopyNeededOptionalStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    var workingMap = context.getWorkingMap();
    Set<String> tableNamesNeedingCopy =
        FlightUtils.getTyped(workingMap, DataDeletionMapKeys.TABLE_NAMES_NEEDING_COPY);
    return !tableNamesNeedingCopy.isEmpty();
  }
}
