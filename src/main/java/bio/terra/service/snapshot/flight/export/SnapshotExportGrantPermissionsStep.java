package bio.terra.service.snapshot.flight.export;

import bio.terra.common.FlightUtils;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.storage.BlobId;
import java.util.List;
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
    return dispatchAclOp(context, AclOp.CREATE_OP);
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return dispatchAclOp(context, AclOp.DELETE_OP);
  }

  private StepResult dispatchAclOp(FlightContext context, AclOp aclOp) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();

    GoogleBucketResource bucketResource =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);
    List<String> paths =
        FlightUtils.getTyped(workingMap, SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_PARQUET_PATHS);
    String exportManifestPath =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_MANIFEST_PATH, String.class);

    List<BlobId> blobs =
        Stream.concat(paths.stream(), Stream.of(exportManifestPath))
            .map(GcsUriUtils::parseBlobUri)
            .collect(Collectors.toList());

    switch (aclOp) {
      case CREATE_OP:
        gcsPdao.setAclOnBlobs(blobs, userRequest, bucketResource);
        break;
      case DELETE_OP:
        gcsPdao.removeAclOnBlobs(blobs, userRequest, bucketResource);
        break;
    }

    return StepResult.getStepResultSuccess();
  }

  private enum AclOp {
    CREATE_OP,
    DELETE_OP;
  }
}
