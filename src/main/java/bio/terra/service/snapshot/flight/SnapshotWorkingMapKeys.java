package bio.terra.service.snapshot.flight;

import bio.terra.common.ProjectCreatingFlightKeys;

public final class SnapshotWorkingMapKeys extends ProjectCreatingFlightKeys {

  private SnapshotWorkingMapKeys() {}

  public static final String SNAPSHOT_ID = "snapshotId";
  public static final String POLICY_MAP = "policyMap";
  public static final String PROJECT_RESOURCE_ID = "projectResourceId";
  public static final String TABLE_ROW_COUNT_MAP = "tableRowCountMap";
}
