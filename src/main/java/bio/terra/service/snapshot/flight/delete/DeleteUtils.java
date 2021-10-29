package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;

public class DeleteUtils {

  public static boolean performGCPStep(FlightContext flightContext) {
    FlightMap map = flightContext.getWorkingMap();
    boolean snapshotExists = map.get(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, boolean.class);
    boolean hasGoogleProject =
        map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_GOOGLE_PROJECT, boolean.class);
    return snapshotExists && hasGoogleProject;
  }

  public static boolean performGCPDatasetDependencyStep(FlightContext flightContext) {
    FlightMap map = flightContext.getWorkingMap();
    boolean datasetExists = map.get(SnapshotWorkingMapKeys.DATASET_EXISTS, boolean.class);
    boolean hasGoogleProject =
        map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_GOOGLE_PROJECT, boolean.class);
    return datasetExists && hasGoogleProject;
  }

  public static boolean performAzureDatasetDependencyStep(FlightContext flightContext) {
    FlightMap map = flightContext.getWorkingMap();
    boolean datasetExists = map.get(SnapshotWorkingMapKeys.DATASET_EXISTS, boolean.class);
    boolean hasAzureStorageAccount =
        map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, boolean.class);
    return datasetExists && hasAzureStorageAccount;
  }

  public static boolean performAzureStep(FlightContext flightContext) {
    FlightMap map = flightContext.getWorkingMap();
    boolean snapshotExists = map.get(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, boolean.class);
    boolean hasAzureStorageAccount =
        map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, boolean.class);
    return snapshotExists && hasAzureStorageAccount;
  }

  public static boolean performSnapshotStep(FlightContext flightContext) {
    FlightMap map = flightContext.getWorkingMap();
    boolean snapshotExists = map.get(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, boolean.class);
    return snapshotExists;
  }

  public static boolean performDatasetStep(FlightContext flightContext) {
    FlightMap map = flightContext.getWorkingMap();
    boolean datasetExists = map.get(SnapshotWorkingMapKeys.DATASET_EXISTS, boolean.class);
    return datasetExists;
  }
}
