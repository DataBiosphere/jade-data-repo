package bio.terra.service.common.azure;

import bio.terra.common.exception.NotImplementedException;
import java.util.UUID;

public enum StorageTableName {
  SNAPSHOT_TABLE("snapshot"),
  LOAD_HISTORY_TABLE("loadHistory"),
  DEPENDENCIES_TABLE("dependencies"),
  DATASET_TABLE("dataset") {
    // TODO - With DR-2127, move remove special case for dataset
    @Override
    public String toTableName(UUID resourceId) {
      throw new NotImplementedException(
          "Dataset storage table names are not unique per resource. Use DATASET_TABLE.toTableName() instead.");
    }

    public String toTableName() {
      return label;
    }
  },
  FILES_TABLE("files") {
    @Override
    public String toTableName(UUID resourceId) {
      throw new NotImplementedException(
          "Files storage table names are not unique per resource. Use FILES_TABLE.toTableName() instead.");
    }

    public String toTableName() {
      return label;
    }
  };

  public final String label;

  StorageTableName(String label) {
    this.label = label;
  }

  /**
   * Generate a Storage Table name from a UUID
   *
   * @param resourceId The datasetId or snapshotId to be used as root of the table name
   * @return A valid azure storage table name
   */
  public String toTableName(UUID resourceId) {
    return "datarepo" + resourceId.toString().replaceAll("-", "") + label;
  }

  // TODO - With DR-2127, move remove special case for dataset
  public String toTableName() {
    throw new IllegalArgumentException("Resource Id must be provided for this storage table type.");
  }
}
