package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.job.OptionalStep;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;

public record PerformAzureDatasetDependencyStep(Step step) implements OptionalStep {
  @Override
  public boolean isEnabled(FlightContext context) {
    FlightMap map = context.getWorkingMap();
    boolean datasetExists = map.get(SnapshotWorkingMapKeys.DATASET_EXISTS, boolean.class);
    boolean hasAzureStorageAccount =
        map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, boolean.class);
    return datasetExists && hasAzureStorageAccount;
  }
}
