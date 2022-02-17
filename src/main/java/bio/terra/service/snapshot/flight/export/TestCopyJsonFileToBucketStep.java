package bio.terra.service.snapshot.flight.export;

import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class TestCopyJsonFileToBucketStep implements Step {

  private final GcsPdao gcsPdao;

  public TestCopyJsonFileToBucketStep(GcsPdao gcsPdao) {
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    String path = "gs://tlangs-test-files/JywTxOzDQfuLILzg7cChHA-firestore-dump.json";
    GoogleBucketResource exportBucket =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);

    String dumpPath =
        GcsUriUtils.getGsPathFromComponents(
            exportBucket.getName(), String.format("%s/dump.json", context.getFlightId()));
    gcsPdao.copyGcsFile(
        GcsUriUtils.parseBlobUri(path),
        GcsUriUtils.parseBlobUri(dumpPath),
        exportBucket.projectIdForBucket());
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_FIRESTORE_DUMP_PATH, dumpPath);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
