package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.FlightUtils;
import bio.terra.common.FutureUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.filedata.FSFile;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.exception.GoogleInternalServerErrorException;
import bio.terra.service.filedata.exception.InvalidUserProjectException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.firestore.FireStoreUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IngestBulkGcpStep extends DefaultUndoStep {

  private Logger logger = LoggerFactory.getLogger(IngestBulkGcpStep.class);

  protected final String loadTag;
  protected final UUID profileId;
  protected final AuthenticatedUserRequest userReq;
  protected final GcsPdao gcsPdao;
  protected final ObjectMapper objectMapper;
  protected final Dataset dataset;
  protected final int maxFailedFileLoads;
  protected final int maxBadLoadFileLineErrorsReported;
  protected final FireStoreDao fileDao;
  protected final FileService fileService;
  protected final ExecutorService executor;
  protected final int maxPerformanceThreadQueueSize;

  // What will ultimately be returned to the end user via flight results
  private final BulkLoadResultModel resultModel = new BulkLoadResultModel();
  private final List<BulkLoadFileResultModel> loadedFiles = new ArrayList<>();

  public IngestBulkGcpStep(
      String loadTag,
      UUID profileId,
      AuthenticatedUserRequest userReq,
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      int maxFailedFileLoads,
      int maxBadLoadFileLineErrorsReported,
      FireStoreDao fileDao,
      FileService fileService,
      ExecutorService executor,
      int maxPerformanceThreadQueueSize) {
    this.loadTag = loadTag;
    this.profileId = profileId;
    this.userReq = userReq;
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
    this.dataset = dataset;
    this.maxFailedFileLoads = maxFailedFileLoads;
    this.maxBadLoadFileLineErrorsReported = maxBadLoadFileLineErrorsReported;
    this.fileDao = fileDao;
    this.fileService = fileService;
    this.executor = executor;
    this.maxPerformanceThreadQueueSize = maxPerformanceThreadQueueSize;
  }

  /**
   * Return a stream of file models representing the files to ingest. The construction of this
   * stream varies based on the type of ingestion flight
   */
  protected abstract Stream<BulkLoadFileModel> getModelsStream(FlightContext context);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {

    initializeResultsModel(context.getFlightId(), loadTag);

    FlightMap workingMap = context.getWorkingMap();

    GoogleBucketResource bucketResource =
        FlightUtils.getContextValue(context, FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    List<FileModel> ingestedFiles;
    try (var loadModels = getModelsStream(context)) {
      ingestedFiles = performIngest(workingMap, loadModels.toList(), bucketResource);
    } catch (InvalidUserProjectException ex) {
      // We retry this exception because often when we've seen this error it has been transient
      // and untruthful -- i.e. the user project specified exists and has a legal id.
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    } catch (GoogleInternalServerErrorException ex) {
      // Google's error message suggests retrying the operation
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    } catch (FileSystemAbortTransactionException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }

    logger.info("Recording results");
    // Must convert to set so that stairway can deserialize
    workingMap.put(IngestMapKeys.COMBINED_EXISTING_FILES, new HashSet<>(ingestedFiles));
    workingMap.put(
        IngestMapKeys.BULK_LOAD_RESULT,
        new BulkLoadArrayResultModel().loadSummary(resultModel).loadFileResults(loadedFiles));

    logger.info("Done ingesting files");
    return StepResult.getStepResultSuccess();
  }

  /** Main driving method that performs the file ingestion */
  private List<FileModel> performIngest(
      FlightMap workingMap, List<BulkLoadFileModel> loadModels, GoogleBucketResource bucketResource)
      throws InterruptedException {

    resultModel.totalFiles(loadModels.size());
    List<BulkLoadFileModel> successfulLoadModels;
    List<FSFileInfo> fsFileInfos = new ArrayList<>();
    Map<String, UUID> fileIdsByPath = new HashMap<>();

    // In the case where file ids are predictable, the linking / copying of the data must happen
    // first in order to obtain file ids
    if (dataset.hasPredictableFileIds()) {
      List<CopyResult> copyResults =
          doFileCopy(workingMap, loadModels, fileIdsByPath, bucketResource);

      copyResults.stream()
          .filter(r -> r.error() == null && r.fsFileInfo() != null)
          .map(CopyResult::fsFileInfo)
          .forEach(fsFileInfos::add);
      copyResults.stream()
          .filter(r -> r.fsFileInfo() != null)
          .forEach(
              r -> fileIdsByPath.put(r.targetPath(), UUID.fromString(r.fsFileInfo().getFileId())));
      successfulLoadModels =
          loadModels.stream().filter(m -> fileIdsByPath.containsKey(m.getTargetPath())).toList();
    } else {
      // Precalculate the fileIds to assign to new file objects (in the predictable fileIds case,
      // this variable gets set in the doFileCopy method)
      loadModels.forEach(m -> fileIdsByPath.put(m.getTargetPath(), UUID.randomUUID()));
      successfulLoadModels = loadModels;
    }

    doCreateFileSystemEntries(successfulLoadModels, fileIdsByPath);

    // If not using predictable file ids, we wait until the end to copy the files (to avoid
    // attempting re-copying files)
    // For the case where we use predictable file Ids, this is a noop
    if (!dataset.hasPredictableFileIds()) {
      List<CopyResult> copyResults =
          doFileCopy(workingMap, loadModels, fileIdsByPath, bucketResource);
      copyResults.stream()
          .filter(r -> r.error() == null && r.fsFileInfo() != null)
          .map(CopyResult::fsFileInfo)
          .forEach(fsFileInfos::add);
      List<String> failedTargetPaths =
          copyResults.stream()
              .filter(r -> r.fsFileInfo() == null)
              .map(CopyResult::targetPath)
              .toList();
      successfulLoadModels =
          loadModels.stream().filter(m -> !failedTargetPaths.contains(m.getTargetPath())).toList();
      fileIdsByPath.entrySet().removeIf(item -> failedTargetPaths.contains(item.getKey()));
    }
    logger.info("Add file metadata to Firestore");
    // Finally, add the file entries
    return doFireStoreFileIngest(successfulLoadModels, fileIdsByPath, fsFileInfos);
  }

  private record CopyResult(
      FSFileInfo fsFileInfo, String sourcePath, String targetPath, Exception error) {}

  /**
   * Link or copy and return the resulting FSFileInfo object . If using predictable Ids, modify
   * fileIdsByPath with the calculated IDs. If not using predictable Ids, use the file Id values
   * from the passed in fileIdsByPath Note: the results of the successfully copied / linked files
   * are stored in the flight's context
   *
   * @param workingMap The flight's working map
   * @param loadModels The models representing each file that the user wants to ingest
   * @param fileIdsByPath A lookup index that maps the virtual path of a file (e.g. what is being
   *     recorded by this method) to the id for this file.
   * @param bucketResource The bucket resource object for the bucket being ingested into
   * @return A list of {@link CopyResult} objects that contain the results of each copy / link
   *     operation (e.g. errors, if present, storage location, etc.)
   */
  private List<CopyResult> doFileCopy(
      FlightMap workingMap,
      List<BulkLoadFileModel> loadModels,
      Map<String, UUID> fileIdsByPath,
      GoogleBucketResource bucketResource) {
    logger.info("Linking / copying %d file(s)".formatted(loadModels.size()));
    List<Future<CopyResult>> futureFsFileInfos = new ArrayList<>(maxPerformanceThreadQueueSize);
    List<CopyResult> copyResults = new ArrayList<>(loadModels.size());

    // Need to batch to make sure that we don't overwhelm the perf threadpool
    for (var batch : ListUtils.partition(loadModels, maxPerformanceThreadQueueSize)) {
      futureFsFileInfos.clear();
      for (var fileLoadModel : batch) {
        UUID fileId;
        if (dataset.hasPredictableFileIds()) {
          fileId = null;
        } else {
          fileId = fileIdsByPath.get(fileLoadModel.getTargetPath());
        }
        if (dataset.isSelfHosted()) {
          futureFsFileInfos.add(
              executor.submit(
                  () -> {
                    try {
                      return new CopyResult(
                          gcsPdao.linkSelfHostedFile(
                              fromBulkFileLoadModel(fileLoadModel),
                              Optional.ofNullable(fileId).map(UUID::toString).orElse(null),
                              dataset.getProjectResource().getGoogleProjectId()),
                          fileLoadModel.getSourcePath(),
                          fileLoadModel.getTargetPath(),
                          null);
                    } catch (Exception e) {
                      logger.warn("Error linking file", e);
                      return new CopyResult(
                          null, fileLoadModel.getSourcePath(), fileLoadModel.getTargetPath(), e);
                    }
                  }));
        } else {
          futureFsFileInfos.add(
              executor.submit(
                  () -> {
                    try {
                      int attemptsLeft = 3;
                      while (true) {
                        try {
                          return new CopyResult(
                              gcsPdao.copyFile(
                                  dataset,
                                  fromBulkFileLoadModel(fileLoadModel),
                                  Optional.ofNullable(fileId).map(UUID::toString).orElse(null),
                                  bucketResource),
                              fileLoadModel.getSourcePath(),
                              fileLoadModel.getTargetPath(),
                              null);
                        } catch (GoogleInternalServerErrorException e) {
                          TimeUnit.SECONDS.sleep(5);
                          attemptsLeft--;
                          if (attemptsLeft == 0) {
                            throw e;
                          }
                        }
                      }
                    } catch (Exception e) {
                      logger.warn("Error copying file", e);
                      return new CopyResult(
                          null, fileLoadModel.getSourcePath(), fileLoadModel.getTargetPath(), e);
                    }
                  }));
        }
      }
      List<CopyResult> results = FutureUtils.waitFor(futureFsFileInfos);
      copyResults.addAll(results);
    }

    long succeededFiles = copyResults.stream().filter(f -> f.error() == null).count();

    resultModel
        .totalFiles(loadModels.size())
        .succeededFiles((int) succeededFiles)
        .failedFiles(loadModels.size() - (int) succeededFiles);

    // Validate that the amount files copied / linked didn't exceed the maximum allowed specified
    // by the user
    validateErrors(workingMap, copyResults.stream().filter(r -> r.error() != null).toList());

    // Save the results of the ingestion to be used to load into the load history table
    List<BulkLoadHistoryModel> loadHistoryModels =
        new ArrayList<>(copyResults.stream().map(this::fromCopyResult).toList());
    workingMap.put(IngestMapKeys.BULK_LOAD_HISTORY_RESULT, loadHistoryModels);

    loadedFiles.addAll(loadHistoryModels.stream().map(this::fromHistoryModel).toList());
    return copyResults;
  }

  /**
   * This method ensures that all filesystem metadata (e.g. data resulting from the target path in
   * the file ingest) gets properly added to the dataset's firestore db
   *
   * @param loadModels The models being ingested whose directories and file entries must be created
   *     Note: in the case of predictable file ids, this will be the subset of file models specified
   *     by the user that were able to be successfully copied
   * @param fileIdsByPath A lookup index that maps the virtual path of a file (e.g. what is being
   *     recorded by this method) to the id for this file. If a conflict is found (e.g. from a
   *     previous load) this map gets mutated and updated with the existing file id
   */
  @VisibleForTesting
  void doCreateFileSystemEntries(
      List<BulkLoadFileModel> loadModels, Map<String, UUID> fileIdsByPath)
      throws InterruptedException {
    logger.info("Extracting Directories");
    // Get all directories to create:
    List<String> directories =
        loadModels.stream()
            .flatMap(l -> FileMetadataUtils.extractDirectoryPaths(l.getTargetPath()).stream())
            .distinct()
            .sorted()
            .toList();

    // Make sure directories are up-to-date (explicitly: NOT the files).  Existing directory
    // entries will be ignored
    logger.info("Upserting directories");
    for (var batch : ListUtils.partition(directories, FireStoreUtils.MAX_FIRESTORE_BATCH_SIZE)) {
      fileDao.upsertDirectoryEntries(dataset, loadTag, batch);
    }

    // Create the leaf (file) nodes
    logger.info("Upserting directory file entries");
    List<FireStoreDirectoryEntry> leafNodes =
        loadModels.stream()
            .map(
                m ->
                    new FireStoreDirectoryEntry()
                        .fileId(fileIdsByPath.get(m.getTargetPath()).toString())
                        .isFileRef(true)
                        .path(FileMetadataUtils.getDirectoryPath(m.getTargetPath()))
                        .name(FileMetadataUtils.getName(m.getTargetPath()))
                        .datasetId(dataset.getId().toString())
                        .loadTag(loadTag))
            .collect(Collectors.toList());

    Map<UUID, UUID> idConflicts = new HashMap<>();
    // Merge ID conflict into fileId map
    for (var batch : ListUtils.partition(leafNodes, FireStoreUtils.MAX_FIRESTORE_BATCH_SIZE)) {
      idConflicts.putAll(fileDao.upsertDirectoryEntries(dataset, batch));
    }

    if (!idConflicts.isEmpty()) {
      logger.info("Found {} id conflicts", idConflicts.size());

      // Cycle through the entries in the file map and replace with existing file ids
      for (var e : fileIdsByPath.entrySet()) {
        UUID newFileId = idConflicts.get(e.getValue());
        if (newFileId != null) {
          e.setValue(newFileId);
        }
      }
    }
  }

  /**
   * Records the basic file metadata entry for a file (e.g. it's size, checksum, real location in
   * the cloud)
   *
   * @param loadModels The models being ingested whose file metadata entries must be created Note:
   *     in the case of predictable file ids, this will be the subset of file models specified by
   *     the user that were able to be successfully copied
   * @param fileIdsByPath A lookup index that maps the virtual path of a file (e.g. what is being
   *     recorded by this method) to the id for this file.
   * @param fsFileInfos A list of the objects that will be recorded in Firestore
   * @return The final list of all files that were recorded in Firestore. This is effectively the
   *     list of successfully ingested files
   */
  private List<FileModel> doFireStoreFileIngest(
      List<BulkLoadFileModel> loadModels,
      Map<String, UUID> fileIdsByPath,
      List<FSFileInfo> fsFileInfos)
      throws InterruptedException {

    Map<String, BulkLoadFileModel> loadModelsById =
        loadModels.stream()
            .collect(
                Collectors.toMap(m -> fileIdsByPath.get(m.getTargetPath()).toString(), m -> m));

    List<FireStoreFile> newFiles =
        fsFileInfos.stream()
            .map(
                fsFileInfo -> {
                  var fileLoadModel = loadModelsById.get(fsFileInfo.getFileId());
                  return new FireStoreFile()
                      .fileId(fsFileInfo.getFileId())
                      .mimeType(fileLoadModel.getMimeType())
                      .description(fileLoadModel.getDescription())
                      .bucketResourceId(fsFileInfo.getBucketResourceId())
                      .fileCreatedDate(fsFileInfo.getCreatedDate())
                      .gspath(fsFileInfo.getCloudPath())
                      .checksumCrc32c(fsFileInfo.getChecksumCrc32c())
                      .checksumMd5(fsFileInfo.getChecksumMd5())
                      .userSpecifiedMd5(fsFileInfo.isUserSpecifiedMd5())
                      .size(fsFileInfo.getSize())
                      .loadTag(loadTag);
                })
            .toList();

    List<List<FireStoreFile>> writeBatches =
        ListUtils.partition(newFiles, FireStoreUtils.MAX_FIRESTORE_BATCH_SIZE);
    int i = 0;
    for (var writeBatch : writeBatches) {
      i++;
      logger.info("Writing batch {} of {}", i, writeBatches.size());
      fileDao.upsertFileMetadata(dataset, writeBatch);
    }
    // Retrieve documents from to build the complete FSItems
    List<FSFile> fsItems = new ArrayList<>(fileIdsByPath.size());
    logger.info("Reading back metadata to return");
    for (var fileIdBatch :
        ListUtils.partition(
            fileIdsByPath.values().stream().map(UUID::toString).toList(),
            FireStoreUtils.MAX_FIRESTORE_BATCH_SIZE)) {
      fsItems.addAll(fileDao.batchRetrieveById(dataset, fileIdBatch, 1));
    }

    return fsItems.stream().map(fileService::fileModelFromFSItem).toList();
  }

  /**
   * Makes sure that not too many file ingest failures occurred
   *
   * @param workingMap The flight's working map
   * @param failedFileLoads A list of all file copy results that failed
   */
  private void validateErrors(FlightMap workingMap, List<CopyResult> failedFileLoads) {
    if (maxFailedFileLoads > -1 && failedFileLoads.size() > maxFailedFileLoads) {
      String errorMessage =
          String.format(
              "More than %d file(s) failed to ingest, which was the allowed amount."
                  + " For a full report, see the load history table for this dataset.",
              maxFailedFileLoads);

      Exception ex =
          new IngestFailureException(
              errorMessage,
              failedFileLoads.stream().map(CopyResult::error).map(Exception::getMessage).toList());
      workingMap.put(CommonMapKeys.COMPLETION_TO_FAILURE_EXCEPTION, ex);
    }
  }

  // Object conversion methods
  private FileLoadModel fromBulkFileLoadModel(BulkLoadFileModel bulkLoadFileModel) {
    return new FileLoadModel()
        .description(bulkLoadFileModel.getDescription())
        .mimeType(bulkLoadFileModel.getMimeType())
        .loadTag(loadTag)
        .targetPath(bulkLoadFileModel.getTargetPath())
        .sourcePath(bulkLoadFileModel.getSourcePath())
        .md5(bulkLoadFileModel.getMd5())
        .profileId(profileId);
  }

  private void initializeResultsModel(String flightId, String loadTag) {
    resultModel
        .jobId(flightId)
        .loadTag(loadTag)
        .totalFiles(0)
        .succeededFiles(0)
        .notTriedFiles(0)
        .failedFiles(0);
  }

  private BulkLoadHistoryModel fromCopyResult(CopyResult copyResult) {
    BulkLoadHistoryModel result =
        new BulkLoadHistoryModel()
            .sourcePath(copyResult.sourcePath())
            .targetPath(copyResult.targetPath());

    if (copyResult.fsFileInfo() != null) {
      result
          .fileId(copyResult.fsFileInfo().getFileId())
          .checksumCRC(copyResult.fsFileInfo().getChecksumCrc32c())
          .checksumMD5(copyResult.fsFileInfo().getChecksumMd5());
    }
    if (copyResult.error() != null) {
      result.error(copyResult.error().getMessage()).state(BulkLoadFileState.FAILED);
    } else {
      result.state(BulkLoadFileState.SUCCEEDED);
    }

    return result;
  }

  private BulkLoadFileResultModel fromHistoryModel(BulkLoadHistoryModel historyModel) {
    return new BulkLoadFileResultModel()
        .fileId(historyModel.getFileId())
        .error(historyModel.getError())
        .state(historyModel.getState())
        .sourcePath(historyModel.getSourcePath())
        .targetPath(historyModel.getTargetPath());
  }
}
