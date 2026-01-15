package bio.terra.service.snapshot.flight.export;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.gcs.GcsChannelWriter;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.WriteChannel;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotExportDumpFirestoreStepTest {

  @Mock private SnapshotService snapshotService;
  @Mock private FireStoreDao fireStoreDao;
  @Mock private GcsPdao gcsPdao;
  @Mock private FlightContext flightContext;
  @Mock private GcsChannelWriter writer;
  @Mock private QueryDocumentSnapshot document1;
  @Mock private QueryDocumentSnapshot document2;

  private SnapshotExportDumpFirestoreStep step;
  private ObjectMapper objectMapper;
  private UUID snapshotId;
  private Snapshot snapshot;
  private GoogleBucketResource exportBucket;

  @BeforeEach
  void setUp() throws Exception {
    snapshotId = UUID.randomUUID();
    objectMapper = new ObjectMapper();
    step =
        new SnapshotExportDumpFirestoreStep(
            snapshotService, fireStoreDao, gcsPdao, snapshotId, objectMapper);

    // Setup snapshot
    UUID datasetId = UUID.randomUUID();
    GoogleProjectResource projectResource =
        new GoogleProjectResource().googleProjectId("test-project");
    Dataset dataset = new Dataset().id(datasetId).projectResource(projectResource);
    snapshot =
        new Snapshot()
            .id(snapshotId)
            .projectResource(projectResource)
            .snapshotSources(List.of(new SnapshotSource().dataset(dataset)));

    // Setup export bucket
    exportBucket = new GoogleBucketResource().name("test-bucket").projectResource(projectResource);
  }

  /** Helper method to set up FlightContext mocks with working map and flight ID. */
  private void setupFlightContext() {
    when(flightContext.getFlightId()).thenReturn("test-flight-id");
    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, exportBucket);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  /**
   * Helper class to hold Storage-related mocks for use in tests. Implements AutoCloseable to allow
   * use in try-with-resources blocks.
   */
  private static class StorageMocks implements AutoCloseable {
    final MockedStatic<StorageOptions> mockedStorageOptions;
    final StorageOptions.Builder mockBuilder;
    final StorageOptions mockStorageOptions;
    final Storage mockStorage;
    final WriteChannel mockWriteChannel;

    StorageMocks(
        MockedStatic<StorageOptions> mockedStorageOptions,
        StorageOptions.Builder mockBuilder,
        StorageOptions mockStorageOptions,
        Storage mockStorage,
        WriteChannel mockWriteChannel) {
      this.mockedStorageOptions = mockedStorageOptions;
      this.mockBuilder = mockBuilder;
      this.mockStorageOptions = mockStorageOptions;
      this.mockStorage = mockStorage;
      this.mockWriteChannel = mockWriteChannel;
    }

    @Override
    public void close() {
      mockedStorageOptions.close();
    }
  }

  /**
   * Helper method to set up Storage client mocks. Returns a StorageMocks object that tests can use
   * to configure WriteChannel behavior and access all mocks. The MockedStatic is automatically
   * closed when StorageMocks is closed in a try-with-resources block.
   */
  private StorageMocks setupStorageMocks() {
    MockedStatic<StorageOptions> mockedStorageOptions = mockStatic(StorageOptions.class);
    StorageOptions.Builder mockBuilder = mock(StorageOptions.Builder.class);
    StorageOptions mockStorageOptions = mock(StorageOptions.class);
    Storage mockStorage = mock(Storage.class);
    WriteChannel mockWriteChannel = mock(WriteChannel.class);

    mockedStorageOptions.when(StorageOptions::newBuilder).thenReturn(mockBuilder);
    when(mockBuilder.setProjectId(anyString())).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockStorageOptions);
    when(mockStorageOptions.getService()).thenReturn(mockStorage);
    when(mockStorage.writer(any(BlobInfo.class))).thenReturn(mockWriteChannel);

    return new StorageMocks(
        mockedStorageOptions, mockBuilder, mockStorageOptions, mockStorage, mockWriteChannel);
  }

  @Test
  void testDoStep_HandlesStorageCreationFailure() throws Exception {
    // Test that the step handles failure when creating Storage client
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    setupFlightContext();

    StepResult result = step.doStep(flightContext);

    // Step fails when creating Storage client, returns RETRY
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
    // Verify snapshot is retrieved before the failure
    verify(snapshotService).retrieve(snapshotId);
  }

  @Test
  void testUndoStep() throws Exception {
    // Setup - undo step doesn't require GCS setup
    setupFlightContext();

    StepResult result = step.undoStep(flightContext);

    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    // Verify deleteFileByName is called - the filename is generated by SnapshotExportUtils
    // Use any() for bucket since object equality might differ
    verify(gcsPdao).deleteFileByName(any(GoogleBucketResource.class), anyString());
  }

  @Test
  void testDoStep_EmptyCollection() throws Exception {
    // Test that empty collections create an empty file (success path)
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    setupFlightContext();

    UUID datasetId = snapshot.getSourceDataset().getId();
    String expectedCollectionName = datasetId.toString() + "-files";

    // Mock StorageOptions and Storage to allow the step to proceed
    try (StorageMocks storageMocks = setupStorageMocks()) {
      // Note: write() is not stubbed since no documents are processed for empty collection

      // Mock pagination to process no documents (empty collection)
      doAnswer(invocation -> null) // No documents processed
          .when(fireStoreDao)
          .processCollectionWithPagination(anyString(), anyString(), any());

      StepResult result = step.doStep(flightContext);

      // Verify success - empty file should be created
      assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
      verify(snapshotService).retrieve(snapshotId);
      // Verify pagination was called with the correct collection name format
      verify(fireStoreDao)
          .processCollectionWithPagination(eq("test-project"), eq(expectedCollectionName), any());
      // Verify write channel is closed even for empty collection
      verify(storageMocks.mockWriteChannel).close();
    }
  }

  @Test
  void testDoStep_SuccessWithMockedStorage() throws Exception {
    // Mock Storage client creation to test the full success path
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    setupFlightContext();

    // Setup mock documents with data - use the actual field names from FireStoreFile
    Map<String, Object> data1 = new HashMap<>();
    data1.put(FireStoreFile.FILE_ID_FIELD_NAME, "file-id-1");
    data1.put(FireStoreFile.GS_PATH_FIELD_NAME, "gs://bucket/file1");
    when(document1.getData()).thenReturn(data1);

    Map<String, Object> data2 = new HashMap<>();
    data2.put(FireStoreFile.FILE_ID_FIELD_NAME, "file-id-2");
    data2.put(FireStoreFile.GS_PATH_FIELD_NAME, "gs://bucket/file2");
    when(document2.getData()).thenReturn(data2);

    // Mock StorageOptions and Storage
    try (StorageMocks storageMocks = setupStorageMocks()) {
      when(storageMocks.mockWriteChannel.write(any(ByteBuffer.class))).thenReturn(1);

      // Mock pagination to process documents
      doAnswer(
              invocation -> {
                bio.terra.service.filedata.google.firestore.InterruptibleConsumer<
                        QueryDocumentSnapshot>
                    consumer = invocation.getArgument(2);
                consumer.accept(document1);
                consumer.accept(document2);
                return null;
              })
          .when(fireStoreDao)
          .processCollectionWithPagination(anyString(), anyString(), any());

      StepResult result = step.doStep(flightContext);

      // Verify success
      assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
      verify(snapshotService).retrieve(snapshotId);
      verify(fireStoreDao)
          .processCollectionWithPagination(
              eq("test-project"),
              eq(snapshot.getSourceDataset().getId().toString() + "-files"),
              any(bio.terra.service.filedata.google.firestore.InterruptibleConsumer.class));
      // Verify documents were written (2 documents = 2 write calls)
      verify(storageMocks.mockWriteChannel, times(2)).write(any(ByteBuffer.class));
      verify(storageMocks.mockWriteChannel).close();
    }
  }

  @Test
  void testDoStep_JsonProcessingException() throws Exception {
    // Test that JsonProcessingException during document serialization returns FATAL
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    setupFlightContext();

    // Setup mock document with data
    Map<String, Object> data1 = new HashMap<>();
    data1.put(FireStoreFile.FILE_ID_FIELD_NAME, "file-id-1");
    data1.put(FireStoreFile.GS_PATH_FIELD_NAME, "gs://bucket/file1");
    when(document1.getData()).thenReturn(data1);

    // Mock StorageOptions and Storage
    try (StorageMocks storageMocks = setupStorageMocks()) {

      // Create an ObjectMapper that will throw JsonProcessingException
      ObjectMapper failingObjectMapper = mock(ObjectMapper.class);
      JsonProcessingException jsonException = new JsonProcessingException("JSON error") {};
      when(failingObjectMapper.writeValueAsString(any())).thenThrow(jsonException);

      // Create step with failing ObjectMapper
      SnapshotExportDumpFirestoreStep failingStep =
          new SnapshotExportDumpFirestoreStep(
              snapshotService, fireStoreDao, gcsPdao, snapshotId, failingObjectMapper);

      // Mock pagination to process a document
      doAnswer(
              invocation -> {
                bio.terra.service.filedata.google.firestore.InterruptibleConsumer<
                        QueryDocumentSnapshot>
                    consumer = invocation.getArgument(2);
                consumer.accept(document1);
                return null;
              })
          .when(fireStoreDao)
          .processCollectionWithPagination(anyString(), anyString(), any());

      StepResult result = failingStep.doStep(flightContext);

      // Verify FATAL error is returned
      assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
      assertThat(result.getException().orElse(null), equalTo(jsonException));
      verify(snapshotService).retrieve(snapshotId);
    }
  }

  @Test
  void testDoStep_IOExceptionDuringWriting() throws Exception {
    // Test that IOException during document writing returns RETRY
    // IOException comes from WriteChannel.write(), not ObjectMapper
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    setupFlightContext();

    // Setup mock document with data
    Map<String, Object> data1 = new HashMap<>();
    data1.put(FireStoreFile.FILE_ID_FIELD_NAME, "file-id-1");
    data1.put(FireStoreFile.GS_PATH_FIELD_NAME, "gs://bucket/file1");
    when(document1.getData()).thenReturn(data1);

    // Mock WriteChannel.write() to throw IOException (this is what GcsChannelWriter.writeLine
    // calls)
    IOException ioException = new IOException("IO error");
    // Mock StorageOptions and Storage
    try (StorageMocks storageMocks = setupStorageMocks()) {
      when(storageMocks.mockWriteChannel.write(any(ByteBuffer.class))).thenThrow(ioException);

      // Mock pagination to process a document
      doAnswer(
              invocation -> {
                bio.terra.service.filedata.google.firestore.InterruptibleConsumer<
                        QueryDocumentSnapshot>
                    consumer = invocation.getArgument(2);
                consumer.accept(document1);
                return null;
              })
          .when(fireStoreDao)
          .processCollectionWithPagination(anyString(), anyString(), any());

      StepResult result = step.doStep(flightContext);

      // Verify RETRY error is returned
      assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
      assertThat(result.getException().orElse(null), equalTo(ioException));
      verify(snapshotService).retrieve(snapshotId);
    }
  }

  @Test
  void testDoStep_RuntimeExceptionWithUnknownCause() throws Exception {
    // Test that RuntimeException with unknown cause (not JsonProcessingException or IOException)
    // returns RETRY when caught by outer catch block
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    setupFlightContext();

    // Mock StorageOptions and Storage
    try (StorageMocks storageMocks = setupStorageMocks()) {

      // Mock pagination to throw RuntimeException with unknown cause (not JsonProcessingException
      // or IOException)
      RuntimeException runtimeException =
          new RuntimeException("Unknown error", new IllegalStateException("Some other error"));
      doThrow(runtimeException)
          .when(fireStoreDao)
          .processCollectionWithPagination(anyString(), anyString(), any());

      StepResult result = step.doStep(flightContext);

      // Verify RETRY error is returned (caught by outer catch block at line 90-91)
      assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
      assertThat(result.getException().orElse(null), equalTo(runtimeException));
      verify(snapshotService).retrieve(snapshotId);
    }
  }

  @Test
  void testDoStep_SnapshotNotFoundException() throws Exception {
    // Test that SnapshotNotFoundException from snapshotService.retrieve propagates
    // (not caught in doStep, so it propagates to Stairway)
    SnapshotNotFoundException notFoundException =
        new SnapshotNotFoundException("Snapshot not found: " + snapshotId);
    when(snapshotService.retrieve(snapshotId)).thenThrow(notFoundException);
    // Note: flightContext.getFlightId() is not stubbed since exception is thrown before it's called

    // Exception should propagate (not caught in doStep)
    SnapshotNotFoundException thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            SnapshotNotFoundException.class, () -> step.doStep(flightContext));

    assertThat(thrown, equalTo(notFoundException));
    verify(snapshotService).retrieve(snapshotId);
  }
}
