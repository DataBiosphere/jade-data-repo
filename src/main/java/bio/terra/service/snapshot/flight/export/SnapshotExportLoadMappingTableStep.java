package bio.terra.service.snapshot.flight.export;

import bio.terra.service.common.gcs.GcsUriUtils;
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

  public SnapshotExportLoadMappingTableStep(
      UUID snapshotId, SnapshotService snapshotService, BigQueryExportPdao bigQueryExportPdao) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
    this.bigQueryExportPdao = bigQueryExportPdao;
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
    bigQueryExportPdao.deleteFirestoreGsPathExternalTable(snapshot, context.getFlightId());
    return StepResult.getStepResultSuccess();
  }
}
