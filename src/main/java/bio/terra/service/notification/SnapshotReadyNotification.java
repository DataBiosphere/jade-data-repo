package bio.terra.service.notification;

public record SnapshotReadyNotification(
    String recipientUserId,
    String snapshotExportLink,
    String snapshotName,
    String snapshotSummary) {}
