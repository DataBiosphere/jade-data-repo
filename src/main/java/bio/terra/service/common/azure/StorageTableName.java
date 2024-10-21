package bio.terra.service.common.azure;

import java.util.Objects;
import java.util.UUID;

public enum StorageTableName {
  SNAPSHOT("snapshot"),
  LOAD_HISTORY("loadHistory"),
  DEPENDENCIES("dependencies"),
  DATASET("dataset"),
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
    Objects.requireNonNull(resourceId, "Resource Id must be provided for this storage table type.");
    return "datarepo" + resourceId.toString().replaceAll("-", "") + label;
  }
}
