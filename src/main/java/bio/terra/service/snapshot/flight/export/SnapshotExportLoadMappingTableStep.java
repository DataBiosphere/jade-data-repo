package bio.terra.service.snapshot.flight.export;

import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryExportPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class SnapshotExportLoadMappingTableStep implements Step {

  private final UUID snapshotId;
  private final SnapshotService snapshotService;
  private final BigQueryExportPdao bigQueryExportPdao;
  private final GcsConfiguration gcsConfiguration;

  public SnapshotExportLoadMappingTableStep(
      UUID snapshotId,
      SnapshotService snapshotService,
      BigQueryExportPdao bigQueryExportPdao,
      GcsConfiguration gcsConfiguration) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
    this.bigQueryExportPdao = bigQueryExportPdao;
    this.gcsConfiguration = gcsConfiguration;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    FlightMap workingMap = context.getWorkingMap();
    String gsPathMappingFile =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_GSPATHS_FILENAME, String.class);

    GoogleBucketResource bucketResource =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);

    String gsPathMappingFilePath =
        GcsUriUtils.getGsPathFromComponents(bucketResource.getName(), gsPathMappingFile);

    bigQueryExportPdao.createFirestoreGsPathExternalTable(
        snapshot, gsPathMappingFilePath, context.getFlightId());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    bigQueryExportPdao.deleteFirestoreGsPathExternalTable(
        snapshot,
        context.getFlightId(),
        gcsConfiguration.getConnectTimeoutSeconds(),
        gcsConfiguration.getReadTimeoutSeconds());
    return StepResult.getStepResultSuccess();
  }
}
