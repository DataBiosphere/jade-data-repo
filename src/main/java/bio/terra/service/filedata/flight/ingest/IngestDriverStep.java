package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.FileLoadModel;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.load.LoadCandidates;
import bio.terra.service.load.LoadFile;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.FlightNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

// The driver step is the core part of the bulk load meta-flight. It runs a loop that keeps some
// K flights busy performing file loads.
//
// We get these elements from the inputs
// - DATASET_ID dataset we are loading into
// - LOAD_TAG is the load tag for this ingest
// - REQUEST is a BulkLoadArrayRequestModel
//
// It expects the following working map data:
// - LOAD_ID - load id we are working on
//
public class IngestDriverStep implements Step {
    private final Logger logger = LoggerFactory.getLogger(IngestDriverStep.class);

    private final LoadService loadService;
    private final String datasetId;
    private final String loadTag;
    private final int concurrentFiles;
    private final int maxFailedFileLoads;
    private final int driverWaitSeconds;
    private final String profileId;

    public IngestDriverStep(LoadService loadService,
                            String datasetId,
                            String loadTag,
                            int concurrentFiles,
                            int maxFailedFileLoads,
                            int driverWaitSeconds,
                            String profileId) {
        this.loadService = loadService;
        this.datasetId = datasetId;
        this.loadTag = loadTag;
        this.concurrentFiles = concurrentFiles;
        this.maxFailedFileLoads = maxFailedFileLoads;
        this.driverWaitSeconds = driverWaitSeconds;
        this.profileId = profileId;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        // Gather inputs
        FlightMap workingMap = context.getWorkingMap();
        String loadIdString = workingMap.get(LoadMapKeys.LOAD_ID, String.class);
        UUID loadId = UUID.fromString(loadIdString);

        try {
            // Check for launch orphans - these are loads in the RUNNING state that never
            // got recorded by stairway.
            checkForOrphans(context, loadId);

            // Load Loop
            while (true) {
                // Get the state of active and failed loads
                LoadCandidates candidates = getLoadCandidates(context, loadId, concurrentFiles);

                int currentRunning = candidates.getRunningLoads().size();
                int candidateCount = candidates.getCandidateFiles().size();
                if (currentRunning == 0 && candidateCount == 0) {
                    // Nothing doing and nothing to do
                    break;
                }

                // Test for exceeding max failed loads; if so, wait for all RUNNINGs to finish
                if (candidates.getFailedLoads() > maxFailedFileLoads) {
                    waitForAll(context, loadId, concurrentFiles);
                    break;
                }

                // Launch new loads
                if (currentRunning < concurrentFiles) {
                    // Compute how many loads to launch
                    int launchCount = concurrentFiles - currentRunning;
                    if (candidateCount < launchCount) {
                        launchCount = candidateCount;
                    }

                    launchLoads(context, launchCount, candidates.getCandidateFiles(), profileId, loadId);
                    currentRunning += launchCount;
                }

                // Wait until some loads complete
                waitForAny(context, loadId, concurrentFiles, currentRunning);
            }
        } catch (DatabaseOperationException ex) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }

    private void waitForAny(FlightContext context, UUID loadId, int concurrentLoads, int originallyRunning)
        throws DatabaseOperationException, InterruptedException {
        while (true) {
            waiting();
            LoadCandidates candidates = getLoadCandidates(context, loadId, concurrentLoads);
            if (candidates.getRunningLoads().size() < originallyRunning) {
                break;
            }
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
        // Check for launch orphans - these are loads in the RUNNING state that are in the load_files table
        // in the database, but not known to Stairway. We revert them to NOT_TRIED before starting the
        // load loop.
        List<LoadFile> runningLoads = loadService.findRunningLoads(loadId);
        for (LoadFile load : runningLoads) {
            try {
                context.getStairway().getFlightState(load.getFlightId());
            } catch (FlightNotFoundException ex) {
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
        logger.debug("Candidates from db: failedLoads={}  runningLoads={}  candidateFiles={}",
            candidates.getFailedLoads(),
            candidates.getRunningLoads().size(),
            candidates.getCandidateFiles().size());

        int failureCount = candidates.getFailedLoads();
        List<LoadFile> realRunningLoads = new LinkedList<>();

        for (LoadFile loadFile : candidates.getRunningLoads()) {
            FlightState flightState = context.getStairway().getFlightState(loadFile.getFlightId());

            switch (flightState.getFlightStatus()) {
                case RUNNING:
                    realRunningLoads.add(loadFile);
                    break;

                case ERROR:
                case FATAL: {
                    String error = "unknown error";
                    if (flightState.getException().isPresent()) {
                        error = flightState.getException().get().toString();
                    }
                    loadService.setLoadFileFailed(loadId, loadFile.getTargetPath(), error);
                    failureCount++;
                    break;
                }

                case SUCCESS: {
                    FlightMap resultMap = flightState.getResultMap().orElse(null);
                    if (resultMap == null) {
                        throw new FileSystemCorruptException("no result map in flight state");
                    }
                    String fileId = resultMap.get(FileMapKeys.FILE_ID, String.class);
                    loadService.setLoadFileSucceeded(loadId, loadFile.getTargetPath(), fileId);
                    break;
                }
            }
        }

        candidates.failedLoads(failureCount).runningLoads(realRunningLoads);
        logger.debug("Candidates resolved: failedLoads={}  runningLoads={}  candidateFiles={}",
            candidates.getFailedLoads(),
            candidates.getRunningLoads().size(),
            candidates.getCandidateFiles().size());

        return candidates;
    }

    private void launchLoads(FlightContext context,
                             int launchCount,
                             List<LoadFile> loadFiles,
                             String profileId,
                             UUID loadId) throws DatabaseOperationException, InterruptedException {
        Stairway stairway = context.getStairway();

        for (int i = 0; i < launchCount; i++) {
            LoadFile loadFile = loadFiles.get(i);
            String flightId = stairway.createFlightId().toString();

            FileLoadModel fileLoadModel = new FileLoadModel()
                .sourcePath(loadFile.getSourcePath())
                .targetPath(loadFile.getTargetPath())
                .mimeType(loadFile.getMimeType())
                .profileId(profileId)
                .loadTag(loadTag)
                .description(loadFile.getDescription());

            FlightMap inputParameters = new FlightMap();
            inputParameters.put(FileMapKeys.DATASET_ID, datasetId);
            inputParameters.put(FileMapKeys.REQUEST, fileLoadModel);

            loadService.setLoadFileRunning(loadId, loadFile.getTargetPath(), flightId);
            // NOTE: this is the window where we have recorded a flight as RUNNING in the load_file
            // table, but it has not yet been launched. A failure in this window leaves "orphan"
            // loads that are marked running, but not actually started. We handle this
            // with the check for launch orphans at the beginning of the do() method.
            stairway.submit(flightId, FileIngestWorkerFlight.class, inputParameters);
        }
    }

}
