package bio.terra.service.snapshot.flight.export;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.storage.BlobId;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SnapshotExportGrantPermissionsStep implements Step {

  private final GcsPdao gcsPdao;
  private final AuthenticatedUserRequest userRequest;

  public SnapshotExportGrantPermissionsStep(GcsPdao gcsPdao, AuthenticatedUserRequest userRequest) {
    this.gcsPdao = gcsPdao;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    return dispatchAclOp(context, GcsPdao.AclOp.ACL_OP_CREATE);
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return dispatchAclOp(context, GcsPdao.AclOp.ACL_OP_DELETE);
  }

  private StepResult dispatchAclOp(FlightContext context, GcsPdao.AclOp aclOp)
      throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();

    GoogleBucketResource bucketResource =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);
    Map<String, List<String>> tablesToPaths =
        FlightUtils.getTyped(workingMap, SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_PARQUET_PATHS);
    String exportManifestPath =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_MANIFEST_PATH, String.class);

    List<BlobId> blobs =
        Stream.concat(
                tablesToPaths.values().stream().flatMap(List::stream),
                Stream.of(exportManifestPath))
            .map(GcsUriUtils::parseBlobUri)
            .collect(Collectors.toList());

    gcsPdao.blobAclUpdates(blobs, userRequest, bucketResource, aclOp);

    return StepResult.getStepResultSuccess();
  }
}
