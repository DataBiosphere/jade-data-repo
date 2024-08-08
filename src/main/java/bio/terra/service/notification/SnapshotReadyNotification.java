package bio.terra.service.notification;

public record SnapshotReadyNotification(
    String notificationType,
    String recipientUserId,
    String snapshotExportLink,
    String snapshotName,
    String snapshotSummary) {
  public SnapshotReadyNotification(
      String recipientUserId,
      String snapshotExportLink,
      String snapshotName,
      String snapshotSummary) {
    this(
        "SnapshotReadyNotification",
        recipientUserId,
        snapshotExportLink,
        snapshotName,
        snapshotSummary);
  }
}
