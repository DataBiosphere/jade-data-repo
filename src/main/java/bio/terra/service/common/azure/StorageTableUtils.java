package bio.terra.service.common.azure;

import java.util.UUID;

public class StorageTableUtils {
  // TODO - With DR-2127, remove these and add case for dataset in toTableName
  public static final String DATASET_TABLE_NAME = "dataset";
  public static final String FILES_TABLE_NAME = "files";

  /**
   * Generate a Storage Table name from a UUID
   *
   * @param resourceId The datasetId or snapshotId to be used as root of the table name
   * @param suffix suffix for storage table Name
   * @return A valid azure storage table name
   */
  public static String toTableName(String resourceId, NameSuffix suffix) {
    return "datarepo" + resourceId.replaceAll("-", "") + suffix.getLabel();
  }

  public static String toTableName(UUID resourceId, NameSuffix suffix) {
    return toTableName(resourceId.toString(), suffix);
  }

  public enum NameSuffix {
    SNAPSHOT("snapshot"),
    LOAD_HISTORY("loadHistory"),
    DEPENDENCIES("dependencies");

    public final String label;

    NameSuffix(String label) {
      this.label = label;
    }

    String getLabel() {
      return this.label;
    }
  }
}
