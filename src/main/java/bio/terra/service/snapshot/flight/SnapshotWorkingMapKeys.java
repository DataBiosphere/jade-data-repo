package bio.terra.service.snapshot.flight;

import bio.terra.common.ProjectCreatingFlightKeys;

public final class SnapshotWorkingMapKeys extends ProjectCreatingFlightKeys {

  private SnapshotWorkingMapKeys() {}

  public static final String SNAPSHOT_ID = "snapshotId";
  public static final String POLICY_MAP = "policyMap";
  public static final String PROJECT_RESOURCE_ID = "projectResourceId";
  public static final String TABLE_ROW_COUNT_MAP = "tableRowCountMap";
  public static final String SNAPSHOT_EXISTS = "snapshotExists";
  public static final String SNAPSHOT_HAS_GOOGLE_PROJECT = "snapshotHasGoogleProject";
  public static final String SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT = "snapshotHasStorageAccount";
  public static final String DATASET_EXISTS = "datasetExists";
  public static final String STORAGE_ACCOUNT_RESOURCE_NAME = "storageAccountResourceName";
  public static final String SNAPSHOT_EXPORT_BUCKET = "snapshotExportBucket";
  public static final String SNAPSHOT_EXPORT_PARQUET_PATHS = "snapshotExportParquetPaths";
  public static final String SNAPSHOT_EXPORT_MANIFEST_PATH = "snapshotExportManifestPath";
}
