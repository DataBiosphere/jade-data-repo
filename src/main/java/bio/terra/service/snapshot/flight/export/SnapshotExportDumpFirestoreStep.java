package bio.terra.service.snapshot.flight.export;

import bio.terra.common.PdaoConstant;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.gcs.GcsChannelWriter;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class SnapshotExportDumpFirestoreStep implements Step {
  private final SnapshotService snapshotService;
  private final FireStoreDao fireStoreDao;
  private final GcsPdao gcsPdao;
  private final UUID snapshotId;
  private final ObjectMapper objectMapper;

  public SnapshotExportDumpFirestoreStep(
      SnapshotService snapshotService,
      FireStoreDao fireStoreDao,
      GcsPdao gcsPdao,
      UUID snapshotId,
      ObjectMapper objectMapper) {
    this.snapshotService = snapshotService;
    this.fireStoreDao = fireStoreDao;
    this.gcsPdao = gcsPdao;
    this.snapshotId = snapshotId;
    this.objectMapper = objectMapper;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    String fileName = SnapshotExportUtils.getFileName(context);
    context.getWorkingMap().put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_GSPATHS_FILENAME, fileName);

    try (GcsChannelWriter writer = makeWriterForDumpFile(context, fileName)) {
      QuerySnapshot queryResult = fireStoreDao.retrieveFilesCollection(snapshot);
      // note, creates an empty file for empty query result
      for (QueryDocumentSnapshot d : queryResult.getDocuments()) {
        // convert to snake_case here to match column names in BQ
        Map<String, Object> dumpData =
            Map.of(
                PdaoConstant.PDAO_FIRESTORE_DUMP_FILE_ID_KEY,
                    d.getData().get(FireStoreFile.FILE_ID_FIELD_NAME),
                PdaoConstant.PDAO_FIRESTORE_DUMP_GSPATH_KEY,
                    d.getData().get(FireStoreFile.GS_PATH_FIELD_NAME));
        try {
          writer.writeLine(objectMapper.writeValueAsString(dumpData));
        } catch (JsonProcessingException e) {
          return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
        }
      }
    } catch (IOException | ExecutionException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }
    return StepResult.getStepResultSuccess();
  }

  private GcsChannelWriter makeWriterForDumpFile(FlightContext context, String fileName) {
    GoogleBucketResource exportBucket =
        context
            .getWorkingMap()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);
    Storage storage =
        StorageOptions.newBuilder()
            .setProjectId(exportBucket.getProjectResource().getGoogleProjectId())
            .build()
            .getService();
    return new GcsChannelWriter(storage, exportBucket.getName(), fileName);
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    GoogleBucketResource exportBucket =
        context
            .getWorkingMap()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);
    gcsPdao.deleteFileByName(exportBucket, SnapshotExportUtils.getFileName(context));
    return StepResult.getStepResultSuccess();
  }
}
