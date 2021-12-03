package bio.terra.service.snapshot.flight.export;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class SnapshotExportWriteManifestStep extends DefaultUndoStep {

  private final UUID snapshotId;
  private final SnapshotService snapshotService;
  private final GcsPdao gcsPdao;
  private final ObjectMapper objectMapper;
  private final AuthenticatedUserRequest userReq;

  public SnapshotExportWriteManifestStep(
      UUID snapshotId,
      SnapshotService snapshotService,
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      AuthenticatedUserRequest userReq) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
    this.userReq = userReq;
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

    List<SnapshotExportResponseModelFormatParquetLocationTables> tables =
        paths.entrySet().stream()
            .map(
                entry ->
                    new SnapshotExportResponseModelFormatParquetLocationTables()
                        .name(entry.getKey())
                        .paths(entry.getValue()))
            .collect(Collectors.toList());

    try {
      String manifestContents =
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(tables);

      gcsPdao.createGcsFile(exportManifestPath, exportBucket.projectIdForBucket());
      gcsPdao.writeStreamToCloudFile(
          exportManifestPath,
          Arrays.stream(manifestContents.split("\n")),
          exportBucket.projectIdForBucket());
    } catch (JsonProcessingException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    SnapshotModel snapshot = snapshotService.retrieveAvailableSnapshotModel(snapshotId, userReq);

    SnapshotExportResponseModel responseModel =
        new SnapshotExportResponseModel()
            .snapshot(snapshot)
            .format(
                new SnapshotExportResponseModelFormat()
                    .parquet(
                        new SnapshotExportResponseModelFormatParquet()
                            .manifest(exportManifestPath)
                            .location(
                                new SnapshotExportResponseModelFormatParquetLocation()
                                    .tables(tables))));

    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_MANIFEST_PATH, exportManifestPath);
    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), responseModel);

    return StepResult.getStepResultSuccess();
  }
}
