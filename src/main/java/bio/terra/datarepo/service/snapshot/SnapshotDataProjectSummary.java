package bio.terra.datarepo.service.snapshot;

import java.util.UUID;

public class SnapshotDataProjectSummary {

  private UUID id;
  private UUID snapshotId;
  private UUID projectResourceId;

  public UUID getId() {
    return id;
  }

  public SnapshotDataProjectSummary id(UUID id) {
    this.id = id;
    return this;
  }

  public UUID getSnapshotId() {
    return snapshotId;
  }

  public SnapshotDataProjectSummary snapshotId(UUID snapshotId) {
    this.snapshotId = snapshotId;
    return this;
  }

  public UUID getProjectResourceId() {
    return projectResourceId;
  }

  public SnapshotDataProjectSummary projectResourceId(UUID projectResourceId) {
    this.projectResourceId = projectResourceId;
    return this;
  }
}
