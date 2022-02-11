package bio.terra.service.snapshot.flight.export;

import bio.terra.service.filedata.google.gcs.GcsChannelWriter;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class SnapshotExportDumpFirestoreStep implements Step {
  private final SnapshotService snapshotService;
  private final UUID snapshotId;
  private final ObjectMapper objectMapper;

  public SnapshotExportDumpFirestoreStep(
      SnapshotService snapshotService, UUID snapshotId, ObjectMapper objectMapper) {
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
    this.objectMapper = objectMapper;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    // String firestoreProject = snapshot.getProjectResource().getGoogleProjectId();
    String firestoreProject =
        firestoreProject =
            snapshot
                .getSnapshotSources()
                .get(0)
                .getDataset()
                .getProjectResource()
                .getGoogleProjectId();

    try {
      FirestoreOptions firestoreOptions =
          FirestoreOptions.getDefaultInstance().toBuilder()
              .setProjectId(firestoreProject)
              .setCredentials(GoogleCredentials.getApplicationDefault())
              .build();
      String sourceDatasetId = snapshot.getSnapshotSources().get(0).getDataset().getId().toString();
      String collectionName = String.format("%s-files", sourceDatasetId);
      final Firestore db = firestoreOptions.getService();
      final CollectionReference collection = db.collection(collectionName);
      QuerySnapshot queryResult = collection.get().get();
      GcsChannelWriter writer = makeWriterForDumpFile(context);
      queryResult
          .getDocuments()
          .forEach(
              d -> {
                final Map<String, Object> data = d.getData();
                try {
                  String serializedData = objectMapper.writeValueAsString(data);
                  writer.write(serializedData);
                } catch (JsonProcessingException e) {
                  // todo: handle this
                  e.printStackTrace();
                } catch (IOException e) {
                  // todo: handle this
                  e.printStackTrace();
                }
              });
      writer.close();
    } catch (IOException | ExecutionException e) {
      // todo: log and retry?
      e.printStackTrace();
    }
    return StepResult.getStepResultSuccess();
  }

  private GcsChannelWriter makeWriterForDumpFile(FlightContext context) {
    GoogleBucketResource exportBucket =
        context
            .getWorkingMap()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, GoogleBucketResource.class);
    String targetPath = getFileName(context);
    Storage storage = StorageOptions.getDefaultInstance().getService();
    return new GcsChannelWriter(storage, exportBucket.getName(), targetPath);
  }

  private String getFileName(FlightContext context) {
    return String.format("%s-firestore-dump.json", context.getFlightId());
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return null;
  }
}
