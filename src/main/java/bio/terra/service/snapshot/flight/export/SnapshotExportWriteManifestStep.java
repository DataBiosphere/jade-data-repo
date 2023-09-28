package bio.terra.service.snapshot.flight.export;

import static bio.terra.service.filedata.google.gcs.GcsConstants.REQUESTED_BY_QUERY_PARAM;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotExportResponseModel;
import bio.terra.model.SnapshotExportResponseModelFormat;
import bio.terra.model.SnapshotExportResponseModelFormatParquet;
import bio.terra.model.SnapshotExportResponseModelFormatParquetLocation;
import bio.terra.model.SnapshotExportResponseModelFormatParquetLocationTables;
import bio.terra.model.SnapshotModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SnapshotExportWriteManifestStep extends DefaultUndoStep {

  private static final Duration URL_TTL = Duration.ofMinutes(60);

  private final UUID snapshotId;
  private final SnapshotService snapshotService;
  private final GcsPdao gcsPdao;
  private final GcsProjectFactory gcsProjectFactory;
  private final ObjectMapper objectMapper;
  private final AuthenticatedUserRequest userReq;
  private final boolean validatePrimaryKeyUniqueness;
  private final boolean signUrls;

  public SnapshotExportWriteManifestStep(
      UUID snapshotId,
      SnapshotService snapshotService,
      GcsPdao gcsPdao,
      GcsProjectFactory gcsProjectFactory,
      ObjectMapper objectMapper,
      AuthenticatedUserRequest userReq,
      boolean validatePrimaryKeyUniqueness,
      boolean signUrls) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
    this.gcsPdao = gcsPdao;
    this.gcsProjectFactory = gcsProjectFactory;
    this.objectMapper = objectMapper;
    this.userReq = userReq;
    this.validatePrimaryKeyUniqueness = validatePrimaryKeyUniqueness;
    this.signUrls = signUrls;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    GoogleBucketResource exportBucket =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);

    Map<String, List<String>> paths =
        FlightUtils.getTyped(workingMap, SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_PARQUET_PATHS);

    String exportManifestPath =
        GcsUriUtils.getGsPathFromComponents(
            exportBucket.getName(), String.format("%s/manifest.json", context.getFlightId()));

    SnapshotModel snapshot = snapshotService.retrieveSnapshotModel(snapshotId, userReq);
    Storage storage = gcsProjectFactory.getStorage(snapshot.getDataProject(), true);
    List<SnapshotExportResponseModelFormatParquetLocationTables> tables =
        paths.entrySet().stream()
            .map(
                entry ->
                    new SnapshotExportResponseModelFormatParquetLocationTables()
                        .name(entry.getKey())
                        .paths(
                            signUrls
                                ? entry.getValue().stream()
                                    .map(path -> signGsUrl(storage, path))
                                    .toList()
                                : entry.getValue()))
            .toList();

    String effectiveExportManifestFile =
        signUrls ? signGsUrl(storage, exportManifestPath) : exportManifestPath;
    SnapshotExportResponseModel responseModel =
        new SnapshotExportResponseModel()
            .snapshot(snapshot)
            .format(
                new SnapshotExportResponseModelFormat()
                    .parquet(
                        new SnapshotExportResponseModelFormatParquet()
                            .manifest(effectiveExportManifestFile)
                            .location(
                                new SnapshotExportResponseModelFormatParquetLocation()
                                    .tables(tables))));

    try {
      String manifestContents =
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(responseModel);

      gcsPdao.createGcsFile(exportManifestPath, exportBucket.projectIdForBucket());
      gcsPdao.writeStreamToCloudFile(
          exportManifestPath,
          Arrays.stream(manifestContents.split("\n")),
          exportBucket.projectIdForBucket());
    } catch (JsonProcessingException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_MANIFEST_PATH, exportManifestPath);
    responseModel.validatedPrimaryKeys(validatePrimaryKeyUniqueness);
    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), responseModel);

    return StepResult.getStepResultSuccess();
  }

  private String signGsUrl(Storage storage, String gsPath) {
    BlobId locator = GcsUriUtils.parseBlobUri(gsPath);
    BlobInfo blobInfo = BlobInfo.newBuilder(locator).build();
    Map<String, String> queryParams = Map.of(REQUESTED_BY_QUERY_PARAM, userReq.getEmail());
    return storage
        .signUrl(
            blobInfo,
            URL_TTL.toMinutes(),
            TimeUnit.MINUTES,
            Storage.SignUrlOption.withQueryParams(queryParams),
            Storage.SignUrlOption.withV4Signature())
        .toString();
  }
}
