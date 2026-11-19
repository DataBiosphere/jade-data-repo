package bio.terra.service.snapshot.flight.export;

import bio.terra.service.common.gcs.BigQueryUtils;
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
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.bigquery.BigQueryException;
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

    // Drop any table left by an earlier attempt, so this step can be retried.
    BigQueryExportPdao.deleteFirestoreGsPathExternalTable(snapshot, context.getFlightId());
    try {
      bigQueryExportPdao.createFirestoreGsPathExternalTable(
          snapshot, gsPathMappingFilePath, context.getFlightId());
    } catch (BigQueryException e) {
      if (BigQueryUtils.isAccessDenied(e)) {
        // The export bucket's IAM grant to TDR can take a few minutes to reach BigQuery.
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
      }
      throw e;
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    bigQueryExportPdao.deleteFirestoreGsPathExternalTable(snapshot, context.getFlightId());
    return StepResult.getStepResultSuccess();
  }
}
