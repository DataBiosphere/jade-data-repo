package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.ErrorCollector;
import bio.terra.common.FutureUtils;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.CloudFileReader;
import bio.terra.service.filedata.exception.BlobAccessNotAuthorizedException;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class IngestPopulateFileStateFromFileStep implements Step {
  private final LoadService loadService;
  private final int maxBadLoadFileLineErrorsReported;
  private final int batchSize;
  private final ObjectMapper bulkLoadObjectMapper;
  private final CloudFileReader cloudFileReader;
  private final ExecutorService executor;
  private final AuthenticatedUserRequest userRequest;
  final Dataset dataset;

  public IngestPopulateFileStateFromFileStep(
      LoadService loadService,
      int maxBadLoadFileLineErrorsReported,
      int batchSize,
      ObjectMapper bulkLoadObjectMapper,
      CloudFileReader cloudFileReader,
      ExecutorService executor,
      AuthenticatedUserRequest userRequest,
      Dataset dataset) {
    this.loadService = loadService;
    this.maxBadLoadFileLineErrorsReported = maxBadLoadFileLineErrorsReported;
    this.batchSize = batchSize;
    this.bulkLoadObjectMapper = bulkLoadObjectMapper;
    this.cloudFileReader = cloudFileReader;
    this.executor = executor;
    this.userRequest = userRequest;
    this.dataset = dataset;
  }

  void readFile(BufferedReader reader, String projectId, FlightContext context) throws IOException {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

    ErrorCollector errorCollector =
        new ErrorCollector(
            maxBadLoadFileLineErrorsReported,
            "Invalid lines in the control file. [All lines in control file must be valid in order to proceed - 'maxFailedFileLoads' not applicable here.]");
    List<Future<BulkLoadFileModel>> futureFiles = new ArrayList<>();

    // Value used in a lambda so needs to be effectively final
    final AtomicLong lineCount = new AtomicLong(0);
    String line;
    while ((line = reader.readLine()) != null) {
      final String lineCopy = line;
      final AtomicBoolean shortCircuit = new AtomicBoolean(false);
      lineCount.incrementAndGet();

      // Run batches in parallel
      futureFiles.add(
          executor.submit(
              () -> {
                try {
                  BulkLoadFileModel loadFile =
                      bulkLoadObjectMapper.readValue(lineCopy, BulkLoadFileModel.class);
                  IngestUtils.validateBulkLoadFileModel(loadFile);
                  cloudFileReader.validateUserCanRead(
                      List.of(loadFile.getSourcePath()), projectId, userRequest, dataset);
                  return loadFile;
                } catch (IOException | BlobAccessNotAuthorizedException | BadRequestException ex) {
                  try {
                    errorCollector.record("Error at line %d: %s", lineCount.get(), ex.getMessage());
                  } catch (BadRequestException e) {
                    // Short circuit throwing.
                    shortCircuit.set(true);
                  }
                  return null;
                }
              }));
      // Keep this check and load out of the inner try; it should only catch objectMapper failures
      if (futureFiles.size() > batchSize) {
        loadService.populateFiles(loadId, FutureUtils.waitFor(futureFiles));
        futureFiles.clear();
      }

      if (shortCircuit.get()) {
        throw errorCollector.getFormattedException();
      }
    }

    if (futureFiles.size() > 0) {
      loadService.populateFiles(loadId, FutureUtils.waitFor(futureFiles));
    }

    // If there are errors in the load file, don't do the load
    if (errorCollector.anyErrorsCollected()) {
      throw errorCollector.getFormattedException();
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

    loadService.cleanFiles(loadId);
    return StepResult.getStepResultSuccess();
  }
}
