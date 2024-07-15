package bio.terra.service.filedata.flight.ingest;

import static bio.terra.common.fixtures.FlightContextFixtures.makeContextMap;
import static bio.terra.service.filedata.DrsService.getLastNameFromPath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.FileLoadModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.StorageException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestBulkGcpStepTest {

  private static final String LOAD_TAG = "loadtag";
  private static final UUID PROFILE_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @Mock private GcsPdao gcsPdao;
  @Mock private ObjectMapper objectMapper;
  @Mock private FireStoreDao fileDao;
  @Mock private FileService fileService;
  @Mock private FlightContext context;

  private ExecutorService executor;
  private Dataset dataset;

  @BeforeEach
  void setUp() throws Exception {
    executor =
        new ThreadPoolExecutor(10, 10, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(100));

    dataset =
        new Dataset()
            .id(UUID.randomUUID())
            .name("test_ds")
            .projectResource(new GoogleProjectResource().googleProjectId("google_project_id"));
  }

  @AfterEach
  void tearDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Test
  void testHappyPathRandomFileIdsNotSelfHosted() throws InterruptedException {
    happyPathTest(false, false);
  }

  @Test
  void testHappyPathPredictableFileIdsNotSelfHosted() throws InterruptedException {
    happyPathTest(true, false);
  }

  @Test
  void testHappyPathRandomFileIdsSelfHosted() throws InterruptedException {
    happyPathTest(false, true);
  }

  @Test
  void testHappyPathPredictableFileIdsSelfHosted() throws InterruptedException {
    happyPathTest(true, true);
  }

  private void happyPathTest(boolean predictableFileIds, boolean selfHosted)
      throws InterruptedException {
    dataset.predictableFileIds(predictableFileIds);
    dataset.getDatasetSummary().selfHosted(selfHosted);

    IngestBulkGcpStep ingestStep =
        initialize(
            List.of(
                new FileToLoad(
                    new BulkLoadFileModel().sourcePath("gs://s/f1").targetPath("/t/f1"), false),
                new FileToLoad(
                    new BulkLoadFileModel().sourcePath("gs://s/f2").targetPath("/t/f2"), false),
                new FileToLoad(
                    new BulkLoadFileModel().sourcePath("gs://s/f3").targetPath("/t/f3"), false)),
            selfHosted);

    StepResult stepResult = ingestStep.doStep(context);
    assertThat(
        "step ran successfully",
        stepResult.getStepStatus(),
        equalTo(StepStatus.STEP_RESULT_SUCCESS));
    assertThat(
        "three files were attempted and all succeeded",
        context
            .getWorkingMap()
            .get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class)
            // Clear load summary for testing
            .loadFileResults(List.of()),
        equalTo(
            new BulkLoadArrayResultModel()
                .loadSummary(
                    new BulkLoadResultModel()
                        .loadTag(LOAD_TAG)
                        .totalFiles(3)
                        .succeededFiles(3)
                        .failedFiles(0)
                        .notTriedFiles(0))
                .loadFileResults(List.of())));

    assertThat(
        "file details (target path) are there to be loaded into load history table",
        context
            .getWorkingMap()
            .get(
                IngestMapKeys.BULK_LOAD_HISTORY_RESULT,
                new TypeReference<List<BulkLoadHistoryModel>>() {})
            .stream()
            .map(BulkLoadHistoryModel::getTargetPath)
            .toList(),
        containsInAnyOrder("/t/f1", "/t/f2", "/t/f3"));
  }

  @Test
  void testFailPathRandomFileIdsNotSelfHosted() throws InterruptedException {
    failPathTest(false, false);
  }

  @Test
  void testFailPathPredictableFileIdsNotSelfHosted() throws InterruptedException {
    failPathTest(true, false);
  }

  @Test
  void testFailPathRandomFileIdsSelfHosted() throws InterruptedException {
    failPathTest(false, true);
  }

  @Test
  void testFailPathPredictableFileIdsSelfHosted() throws InterruptedException {
    failPathTest(true, true);
  }

  void failPathTest(boolean predictableFileIds, boolean selfHosted) throws InterruptedException {
    dataset.predictableFileIds(predictableFileIds);
    dataset.getDatasetSummary().selfHosted(selfHosted);

    IngestBulkGcpStep ingestStep =
        initialize(
            List.of(
                new FileToLoad(
                    new BulkLoadFileModel().sourcePath("gs://s/f1").targetPath("/t/f1"), false),
                new FileToLoad(
                    new BulkLoadFileModel().sourcePath("gs://s/f2").targetPath("/t/f2"), true),
                new FileToLoad(
                    new BulkLoadFileModel().sourcePath("gs://s/f3").targetPath("/t/f3"), false)),
            selfHosted);

    StepResult stepResult = ingestStep.doStep(context);
    assertThat(
        "step ran successfully",
        stepResult.getStepStatus(),
        equalTo(StepStatus.STEP_RESULT_SUCCESS));
    assertThat(
        "three files were attempted and all succeeded",
        context
            .getWorkingMap()
            .get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class)
            // Clear load summary for testing
            .loadFileResults(List.of()),
        equalTo(
            new BulkLoadArrayResultModel()
                .loadSummary(
                    new BulkLoadResultModel()
                        .loadTag(LOAD_TAG)
                        .totalFiles(3)
                        .succeededFiles(2)
                        .failedFiles(1)
                        .notTriedFiles(0))
                .loadFileResults(List.of())));

    List<BulkLoadHistoryModel> loadHistoryModels =
        context
            .getWorkingMap()
            .get(IngestMapKeys.BULK_LOAD_HISTORY_RESULT, new TypeReference<>() {});

    assertThat(
        "file details (target path) are there to be loaded into load history table",
        loadHistoryModels.stream().map(BulkLoadHistoryModel::getTargetPath).toList(),
        containsInAnyOrder("/t/f1", "/t/f2", "/t/f3"));

    loadHistoryModels.forEach(
        m -> {
          if (m.getTargetPath().equals("/t/f2")) {
            assertThat("the failing file has an error", m.getError(), equalTo("Error from Google"));
          } else {
            assertThat("succeeding files have not error", m.getError(), emptyOrNullString());
          }
        });
  }

  @Test
  void testIdConflicts() throws InterruptedException {
    IngestBulkGcpStep ingestStep =
        initialize(
            List.of(
                new FileToLoad(
                    new BulkLoadFileModel().sourcePath("gs://s/f1").targetPath("/t/f1"), false),
                new FileToLoad(
                    new BulkLoadFileModel().sourcePath("gs://s/f2").targetPath("/t/f2"), false),
                new FileToLoad(
                    new BulkLoadFileModel().sourcePath("gs://s/f3").targetPath("/t/f3"), false)),
            false);

    when(fileDao.upsertDirectoryEntries(eq(dataset), eq("loadtag"), any())).thenReturn(Map.of());

    UUID existingFileId = UUID.randomUUID();
    // Mock so that the 2 second file (gs://s/f2) is a conflict
    when(fileDao.upsertDirectoryEntries(eq(dataset), any()))
        .thenAnswer(
            a -> {
              List<FireStoreDirectoryEntry> newEntries = a.getArgument(1);
              return Map.of(UUID.fromString(newEntries.get(1).getFileId()), existingFileId);
            });
    StepResult stepResult = ingestStep.doStep(context);
    assertThat(
        "step ran successfully",
        stepResult.getStepStatus(),
        equalTo(StepStatus.STEP_RESULT_SUCCESS));

    List<BulkLoadHistoryModel> loadHistoryModels =
        context
            .getWorkingMap()
            .get(IngestMapKeys.BULK_LOAD_HISTORY_RESULT, new TypeReference<>() {});

    BulkLoadHistoryModel preExistingFile =
        loadHistoryModels.stream()
            .filter(m -> m.getTargetPath().equals("/t/f2"))
            .findFirst()
            .orElseThrow();

    assertThat(
        "the existing id is reused in case of collision",
        preExistingFile.getFileId(),
        equalTo(existingFileId.toString()));
  }

  private record FileToLoad(BulkLoadFileModel model, boolean shouldFail) {}

  private IngestBulkGcpStep initialize(List<FileToLoad> files, boolean selfHosted) {
    // Mock common flight context
    when(context.getInputParameters())
        .thenReturn(makeContextMap(Map.of(FileMapKeys.BUCKET_INFO, new GoogleBucketResource())));
    FlightMap workingMap = new FlightMap();
    when(context.getWorkingMap()).thenReturn(workingMap);

    if (!selfHosted) {
      // Mock the copy operation for the files
      when(gcsPdao.copyFile(eq(dataset), any(), any(), any()))
          .thenAnswer(
              a -> {
                Dataset dataset = a.getArgument(0);
                FileLoadModel fileLoadModel = a.getArgument(1);
                boolean shouldFail =
                    files.stream()
                        .filter(
                            f ->
                                f.model().getSourcePath().equals(fileLoadModel.getSourcePath())
                                    && f.model()
                                        .getTargetPath()
                                        .equals(fileLoadModel.getTargetPath()))
                        .findFirst()
                        .map(FileToLoad::shouldFail)
                        .orElse(true);
                String fileId = a.getArgument(2);
                GoogleBucketResource bucketResource = a.getArgument(3);

                if (shouldFail) {
                  throw new StorageException(500, "Error from Google");
                }

                String hashingString =
                    fileLoadModel.getSourcePath() + fileLoadModel.getTargetPath();
                if (dataset.hasPredictableFileIds()) {
                  fileId =
                      UUID.nameUUIDFromBytes(hashingString.getBytes(Charsets.UTF_8)).toString();
                }
                return new FSFileInfo()
                    .fileId(fileId)
                    .cloudPath(
                        "gs://%s/%s/%s/%s"
                            .formatted(
                                bucketResource.getName(),
                                dataset.getId().toString(),
                                fileId,
                                getLastNameFromPath(fileLoadModel.getTargetPath())))
                    .checksumMd5(DigestUtils.md5Hex(hashingString))
                    .size((long) hashingString.length())
                    .createdDate(Instant.now().toString());
              });
    }

    if (selfHosted) {
      // Mock the copy operation for the files
      when(gcsPdao.linkSelfHostedFile(any(), any(), any()))
          .thenAnswer(
              a -> {
                FileLoadModel fileLoadModel = a.getArgument(0);
                boolean shouldFail =
                    files.stream()
                        .filter(
                            f ->
                                f.model().getSourcePath().equals(fileLoadModel.getSourcePath())
                                    && f.model()
                                        .getTargetPath()
                                        .equals(fileLoadModel.getTargetPath()))
                        .findFirst()
                        .map(FileToLoad::shouldFail)
                        .orElse(true);
                String fileId = a.getArgument(1);

                if (shouldFail) {
                  throw new StorageException(500, "Error from Google");
                }

                String hashingString =
                    fileLoadModel.getSourcePath() + fileLoadModel.getTargetPath();
                if (dataset.hasPredictableFileIds()) {
                  fileId =
                      UUID.nameUUIDFromBytes(hashingString.getBytes(Charsets.UTF_8)).toString();
                }
                return new FSFileInfo()
                    .fileId(fileId)
                    .cloudPath(fileLoadModel.getSourcePath())
                    .checksumMd5(DigestUtils.md5Hex(hashingString))
                    .size((long) hashingString.length())
                    .createdDate(Instant.now().toString());
              });
    }

    // Instantiate the step
    return new IngestBulkGcpStep(
        LOAD_TAG,
        PROFILE_ID,
        TEST_USER,
        gcsPdao,
        objectMapper,
        dataset,
        0,
        0,
        fileDao,
        fileService,
        executor,
        10) {
      @Override
      protected Stream<BulkLoadFileModel> getModelsStream(FlightContext context) {
        return files.stream().map(FileToLoad::model);
      }
    };
  }
}
