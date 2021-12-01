package bio.terra.service.snapshot.flight.export;

import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class SnapshotExportCreateParquetFilesStep extends DefaultUndoStep {

  private final BigQueryPdao bigQueryPdao;
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public SnapshotExportCreateParquetFilesStep(
      BigQueryPdao bigQueryPdao, SnapshotService snapshotService, UUID snapshotId) {
    this.bigQueryPdao = bigQueryPdao;
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    GoogleBucketResource exportBucket =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);

    List<String> paths =
        bigQueryPdao.exportTableToParquet(snapshot, exportBucket, context.getFlightId());

    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_PARQUET_PATHS, paths);

    return StepResult.getStepResultSuccess();
  }
}
