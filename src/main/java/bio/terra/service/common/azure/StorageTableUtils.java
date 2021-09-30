package bio.terra.service.common.azure;

import java.util.UUID;

public class StorageTableUtils {
  // TODO - With DR-2127, remove these and add case for dataset in toTableName
  public static final String DATASET_TABLE_NAME = "dataset";
  public static final String FILES_TABLE_NAME = "files";

  public enum NameSuffix {
    SNAPSHOT("snapshot"),
    LOAD_HISTORY("loadHistory"),
    DEPENDENCIES("dependencies");

    public final String label;

    NameSuffix(String label) {
      this.label = label;
    }

    /**
     * Generate a Storage Table name from a UUID
     *
     * @param resourceId The datasetId or snapshotId to be used as root of the table name
     * @return A valid azure storage table name
     */
    public String toTableName(UUID resourceId) {
      return "datarepo" + resourceId.toString().replaceAll("-", "") + this.label;
    }
  }
}
