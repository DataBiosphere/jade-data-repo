package bio.terra.service.common.azure;

import bio.terra.common.exception.NotImplementedException;
import java.security.InvalidParameterException;
import java.util.UUID;

public enum StorageTableName {
  SNAPSHOT_TABLE("snapshot"),
  LOAD_HISTORY_TABLE("loadHistory"),
  DEPENDENCIES_TABLE("dependencies"),
  DATASET_TABLE("dataset"),
  FILES_TABLE("files");

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
    switch (this) {
        // TODO - With DR-2127, move remove special case for dataset
      case DATASET_TABLE:
      case FILES_TABLE:
        throw new NotImplementedException(
            "We do not yet handle a resource specific table name for " + this.label);
      default:
        return "datarepo" + resourceId.toString().replaceAll("-", "") + this.label;
    }
  }

  // TODO - With DR-2127, move remove special case for dataset
  public String toTableName() {
    switch (this) {
      case DATASET_TABLE:
      case FILES_TABLE:
        return this.label;
      default:
        throw new InvalidParameterException(
            this.label + " table type requires passing in a resourceId");
    }
  }
}
