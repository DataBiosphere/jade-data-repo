package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.FileLoadModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadCandidates;
import bio.terra.service.load.LoadFile;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.DuplicateFlightIdException;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.StairwayExecutionException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// The driver step is the core part of the bulk load meta-flight. It runs a loop that keeps some
// K flights busy performing file loads.
//
// We get these elements from the inputs
// - DATASET_ID dataset we are loading into
// - LOAD_TAG is the load tag for this ingest
// - REQUEST is a BulkLoadArrayRequestModel
// - CLOUD_PLATFORM is a CloudPlatform
//
// It expects the following working map data:
// - LOAD_ID - load id we are working on
// - BUCKET_INFO is a GoogleBucketResource
// - STORAGE_ACCOUNT_RESOURCE is a AzureStorageAccountResource
//
public class IngestDriverStep extends DefaultUndoStep {
  private static final Logger logger = LoggerFactory.getLogger(IngestDriverStep.class);

  private final LoadService loadService;
  private final ConfigurationService configurationService;
  private final JobService jobService;
  private final String datasetId;
  private final String loadTag;
  private final int maxFailedFileLoads;
  private final int driverWaitSeconds;
  private final UUID profileId;
  private final CloudPlatform platform;
  private final AuthenticatedUserRequest userReq;

  public IngestDriverStep(
      LoadService loadService,
      ConfigurationService configurationService,
      JobService jobService,
      String datasetId,
      String loadTag,
      int maxFailedFileLoads,
      int driverWaitSeconds,
      UUID profileId,
      CloudPlatform platform,
      AuthenticatedUserRequest userReq) {
    this.loadService = loadService;
    this.configurationService = configurationService;
    this.jobService = jobService;
    this.datasetId = datasetId;
    this.loadTag = loadTag;
    this.maxFailedFileLoads = maxFailedFileLoads;
    this.driverWaitSeconds = driverWaitSeconds;
    this.profileId = profileId;
    this.platform = platform;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    // Gather inputs
    FlightMap workingMap = context.getWorkingMap();
    String loadIdString = workingMap.get(LoadMapKeys.LOAD_ID, String.class);
    UUID loadId = UUID.fromString(loadIdString);

    GoogleBucketResource bucketResource =
        workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    BillingProfileModel billingProfileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccountResource =
        workingMap.get(
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);

    int concurrentFiles = configurationService.getParameterValue(ConfigEnum.LOAD_CONCURRENT_FILES);
    boolean maxBadRecordsReached = false;

    try {
      // Check for launch orphans - these are loads in the RUNNING state that never
      // got recorded by stairway.
      checkForOrphans(context, loadId);

      // Load Loop
      while (true) {
        int podCount = jobService.getActivePodCount();
        int scaledConcurrentFiles = podCount * concurrentFiles;
        // Get the state of active and failed loads
        LoadCandidates candidates = getLoadCandidates(context, loadId, scaledConcurrentFiles);

        int currentRunning = candidates.getRunningLoads().size();
        int candidateCount = candidates.getCandidateFiles().size();

        if (maxFailedFileLoads != -1 && candidates.getFailedLoads() > maxFailedFileLoads) {
          maxBadRecordsReached = true;
        }

        if (currentRunning == 0 && candidateCount == 0) {
          // Nothing doing and nothing to do
          break;
        }

        // Test for exceeding max failed loads; if so, wait for all RUNNINGs to finish
        // If we exceed tha max failed loads, fail the flight.
        if (maxBadRecordsReached) {
          waitForAll(context, loadId, scaledConcurrentFiles);
          break;
        }

        // Launch new loads
        if (currentRunning < scaledConcurrentFiles) {
          // Compute how many loads to launch
          int launchCount = scaledConcurrentFiles - currentRunning;
          if (candidateCount < launchCount) {
            launchCount = candidateCount;
          }

          launchLoads(
              context,
              userReq,
              launchCount,
              candidates.getCandidateFiles(),
              profileId,
              loadId,
              bucketResource,
              billingProfileModel,
              storageAccountResource,
              platform);

          currentRunning += launchCount;
        }

        // Wait until some loads complete
        waitForAny(context, loadId, scaledConcurrentFiles, currentRunning);
      }
    } catch (DatabaseOperationException
        | StairwayExecutionException
        | DuplicateFlightIdException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    }

    if (maxBadRecordsReached) {
      String errorMessage =
          String.format(
              "More than %d file(s) failed to ingest, which was the allowed amount."
                  + " See error details for the first %d error(s). "
                  + " For a full report, see the load history table for this dataset.",
              maxFailedFileLoads, concurrentFiles);

      List<String> loadErrors =
          loadService.getFailedLoads(loadId, concurrentFiles).stream()
              .map(lf -> lf.getSourcePath() + " -> " + lf.getTargetPath() + ": " + lf.getError())
              .collect(Collectors.toList());

      Exception ex = new IngestFailureException(errorMessage, loadErrors);
      workingMap.put(CommonMapKeys.COMPLETION_TO_FAILURE_EXCEPTION, ex);
    }
    return StepResult.getStepResultSuccess();
  }

  private void waitForAny(
      FlightContext context, UUID loadId, int concurrentLoads, int originallyRunning)
      throws DatabaseOperationException, InterruptedException, DuplicateFlightIdException {
    while (true) {
      // This code used to wait before getting load candidates again. however,
      // when there are a large number of files being loaded, there is always something completing.
      // So we recheck right away before waiting and only wait if we did not find any new work to do
      LoadCandidates candidates = getLoadCandidates(context, loadId, concurrentLoads);
      if (candidates.getRunningLoads().size() < originallyRunning) {
        break;
      }
      waiting();
    }
  }

  private void waitForAll(FlightContext context, UUID loadId, int concurrentLoads)
      throws DatabaseOperationException, InterruptedException {
    while (true) {
      waiting();
      LoadCandidates candidates = getLoadCandidates(context, loadId, concurrentLoads);
      if (candidates.getRunningLoads().size() == 0) {
        break;
      }
    }
  }

  private void waiting() throws InterruptedException {
    logger.debug("Waiting for file loads to complete...");
    TimeUnit.SECONDS.sleep(driverWaitSeconds);
  }

  private void checkForOrphans(FlightContext context, UUID loadId)
      throws DatabaseOperationException, InterruptedException {
    // Check for launch orphans - these are loads in the RUNNING state in the load_files table
    // in the database, but not known to Stairway. We revert them to NOT_TRIED before starting the
    // load loop.
    List<LoadFile> runningLoads = loadService.findRunningLoads(loadId);
    for (LoadFile load : runningLoads) {
      try {
        context.getStairway().getFlightState(load.getFlightId());
      } catch (FlightNotFoundException ex) {
        logger.debug("Resetting orphan file load from running to not tried: " + load.getLoadId());
        loadService.setLoadFileNotTried(load.getLoadId(), load.getTargetPath());
      }
    }
  }

  private LoadCandidates getLoadCandidates(FlightContext context, UUID loadId, int concurrentLoads)
      throws DatabaseOperationException, InterruptedException {
    // We start by getting the database view of the state of loads.
    // For the running loads, we ask Stairway what the actual state is.
    // If they have completed, we mark them as such.
    // We then update the failure count and runnings loads list in the
    // LoadCandidates so it correctly reflects the running state
    // right now (more or less).
    LoadCandidates candidates = loadService.findCandidates(loadId, concurrentLoads);
    logger.debug(
        "Candidates from db: failedLoads={}  runningLoads={}  candidateFiles={}",
        candidates.getFailedLoads(),
        candidates.getRunningLoads().size(),
        candidates.getCandidateFiles().size());

    int failureCount = candidates.getFailedLoads();
    List<LoadFile> realRunningLoads = new LinkedList<>();

    for (LoadFile loadFile : candidates.getRunningLoads()) {
      FlightState flightState = context.getStairway().getFlightState(loadFile.getFlightId());

      switch (flightState.getFlightStatus()) {
        case RUNNING:
        case WAITING:
        case READY:
        case QUEUED:
          logger.debug("~~running load - flight: " + flightState.getFlightId());
          realRunningLoads.add(loadFile);
          break;

        case ERROR:
        case FATAL:
          {
            logger.debug("~~error load - flight: " + flightState.getFlightId());
            String error = "unknown error";
            if (flightState.getException().isPresent()) {
              error = flightState.getException().get().toString();
            }
            loadService.setLoadFileFailed(loadId, loadFile.getTargetPath(), error);
            failureCount++;
            break;
          }

        case SUCCESS:
          {
            logger.debug("~~success load - flight: " + flightState.getFlightId());
            FlightMap resultMap = flightState.getResultMap().orElse(null);
            if (resultMap == null) {
              throw new FileSystemCorruptException("no result map in flight state");
            }
            String fileId = resultMap.get(FileMapKeys.FILE_ID, String.class);
            FSFileInfo fileInfo = resultMap.get(FileMapKeys.FILE_INFO, FSFileInfo.class);
            loadService.setLoadFileSucceeded(loadId, loadFile.getTargetPath(), fileId, fileInfo);
            break;
          }

        default:
          throw new CorruptMetadataException(
              "Invalid flight state: " + flightState.getFlightStatus());
      }
    }

    candidates.failedLoads(failureCount).runningLoads(realRunningLoads);
    logger.debug(
        "Candidates resolved: failedLoads={}  runningLoads={}  candidateFiles={}",
        candidates.getFailedLoads(),
        candidates.getRunningLoads().size(),
        candidates.getCandidateFiles().size());

    return candidates;
  }

  private void launchLoads(
      FlightContext context,
      AuthenticatedUserRequest userReq,
      int launchCount,
      List<LoadFile> loadFiles,
      UUID profileId,
      UUID loadId,
      GoogleBucketResource bucketInfo,
      BillingProfileModel billingProfileModel,
      AzureStorageAccountResource storageAccountResource,
      CloudPlatform platform)
      throws DatabaseOperationException, StairwayExecutionException, InterruptedException {

    Stairway stairway = context.getStairway();

    for (int i = 0; i < launchCount; i++) {
      LoadFile loadFile = loadFiles.get(i);
      String flightId = stairway.createFlightId();

      FileLoadModel fileLoadModel =
          new FileLoadModel()
              .sourcePath(loadFile.getSourcePath())
              .targetPath(loadFile.getTargetPath())
              .mimeType(loadFile.getMimeType())
              .profileId(profileId)
              .loadTag(loadTag)
              .description(loadFile.getDescription());

      FlightMap inputParameters = new FlightMap();
      inputParameters.put(FileMapKeys.DATASET_ID, datasetId);
      inputParameters.put(FileMapKeys.REQUEST, fileLoadModel);
      inputParameters.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), userReq);
      inputParameters.put(FileMapKeys.BUCKET_INFO, bucketInfo);
      inputParameters.put(ProfileMapKeys.PROFILE_MODEL, billingProfileModel);
      inputParameters.put(CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, storageAccountResource);
      inputParameters.put(JobMapKeys.CLOUD_PLATFORM.getKeyName(), platform.name());
      inputParameters.put(JobMapKeys.PARENT_FLIGHT_ID.getKeyName(), context.getFlightId());

      if (platform == CloudPlatform.AZURE) {
        AzureStorageAuthInfo storageAuthInfo =
            AzureStorageAuthInfo.azureStorageAuthInfoBuilder(
                billingProfileModel, storageAccountResource);
        inputParameters.put(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, storageAuthInfo);
      }

      logger.debug("~~set running load - flight: " + flightId);
      loadService.setLoadFileRunning(loadId, loadFile.getTargetPath(), flightId);
      // NOTE: this is the window where we have recorded a flight as RUNNING in the load_file
      // table, but it has not yet been launched. A failure in this window leaves "orphan"
      // loads that are marked running, but not actually started. We handle this
      // with the check for launch orphans at the beginning of the do() method.
      // We use submitToQueue to spread the file loaders across multiple instances of datarepo.
      stairway.submitToQueue(flightId, FileIngestWorkerFlight.class, inputParameters);
    }
  }
}
