package bio.terra.service.snapshot.flight.export;

import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryExportPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public record SnapshotExportCreateParquetFilesStep(
    BigQueryExportPdao bigQueryExportPdao,
    GcsPdao gcsPdao,
    SnapshotService snapshotService,
    UUID snapshotId,
    boolean exportGsPaths)
    implements DefaultUndoStep {

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    String flightId = context.getFlightId();
    FlightMap workingMap = context.getWorkingMap();
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    GoogleBucketResource exportBucket =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);

    List<String> paths =
        bigQueryExportPdao.exportTableToParquet(snapshot, exportBucket, flightId, exportGsPaths);

    Map<String, List<String>> tablesToPaths =
        paths.stream()
            .map(
                path -> {
                  // Path will always be gs://<bucket>/<flightId>/<tableName>/<parquetFiles*>
                  String tableName = path.split("/")[4];
                  List<String> files =
                      gcsPdao
                          .listGcsIngestBlobs(path + "/*", exportBucket.projectIdForBucket())
                          .stream()
                          .map(GcsUriUtils::getGsPathFromBlob)
                          .collect(Collectors.toList());
                  return Map.entry(tableName, files);
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_PARQUET_PATHS, tablesToPaths);

    return StepResult.getStepResultSuccess();
  }
}
