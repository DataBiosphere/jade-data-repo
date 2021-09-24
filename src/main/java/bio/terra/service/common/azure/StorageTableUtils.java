package bio.terra.service.common.azure;

import java.util.UUID;

public class StorageTableUtils {
  private static final String DATASET_TABLE_NAME = "dataset";

  /**
   * Generate a Storage Table name from a UUID
   *
   * @param resourceId The datasetId or snapshotId to be used as root of the table name
   * @param suffix suffix for storage table Name
   * @return A valid azure storage table name
   */
  public static String toTableName(String resourceId, StorageTableNameSuffix suffix) {
    return "datarepo_" + resourceId.replaceAll("-", "") + suffix.getLabel();
  }

  public static String toTableName(UUID resourceId, StorageTableNameSuffix suffix) {
    return toTableName(resourceId.toString(), suffix);
  }

  // TODO - With DR-2127, remove this  method and add case for dataset in toTableName
  public static String getDatasetTableName() {
    return DATASET_TABLE_NAME;
  }

  public enum StorageTableNameSuffix {
    SNAPSHOT("_snapshot"),
    LOAD_HISTORY("_loadHistory");

    public final String label;

    StorageTableNameSuffix(String label) {
      this.label = label;
    }

    String getLabel() {
      return this.label;
    }
  }
}
