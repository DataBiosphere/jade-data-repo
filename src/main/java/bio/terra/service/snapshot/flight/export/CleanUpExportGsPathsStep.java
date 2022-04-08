package bio.terra.service.snapshot.flight.export;

import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryExportPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class CleanUpExportGsPathsStep implements Step {

  private final BigQueryExportPdao bigQueryExportPdao;
  private final GcsPdao gcsPdao;
  private final SnapshotService snapshotService;
  private final UUID snapshotId;
  private final GcsConfiguration gcsConfiguration;

  public CleanUpExportGsPathsStep(
      BigQueryExportPdao bigQueryExportPdao,
      GcsPdao gcsPdao,
      SnapshotService snapshotService,
      UUID snapshotId,
      GcsConfiguration gcsConfiguration) {

    this.bigQueryExportPdao = bigQueryExportPdao;
    this.gcsPdao = gcsPdao;
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
    this.gcsConfiguration = gcsConfiguration;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    bigQueryExportPdao.deleteFirestoreGsPathExternalTable(
        snapshot,
        context.getFlightId(),
        gcsConfiguration.getConnectTimeoutSeconds(),
        gcsConfiguration.getReadTimeoutSeconds());

    GoogleBucketResource exportBucket =
        context
            .getWorkingMap()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);
    gcsPdao.deleteFileByName(exportBucket, SnapshotExportUtils.getFileName(context));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
