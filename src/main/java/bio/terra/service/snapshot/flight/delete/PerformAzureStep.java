package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.job.OptionalStep;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;

public class PerformAzureStep extends OptionalStep {

  public PerformAzureStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    FlightMap map = context.getWorkingMap();
    boolean snapshotExists = map.get(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, boolean.class);
    boolean hasAzureStorageAccount =
        map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, boolean.class);
    return snapshotExists && hasAzureStorageAccount;
  }
}
