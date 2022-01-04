package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.filedata.exception.BlobAccessNotAuthorizedException;
import bio.terra.service.filedata.exception.BulkLoadControlFileException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
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

public abstract class IngestPopulateFileStateFromFileStep implements Step {
  private final LoadService loadService;
  private final int maxBadLoadFileLineErrorsReported;
  private final int batchSize;
  private final ObjectMapper bulkLoadObjectMapper;
  private final GcsPdao gcsPdao;
  private final AuthenticatedUserRequest userRequest;

  public IngestPopulateFileStateFromFileStep(
      LoadService loadService,
      int maxBadLoadFileLineErrorsReported,
      int batchSize,
      ObjectMapper bulkLoadObjectMapper,
      GcsPdao gcsPdao,
      AuthenticatedUserRequest userRequest) {
    this.loadService = loadService;
    this.maxBadLoadFileLineErrorsReported = maxBadLoadFileLineErrorsReported;
    this.batchSize = batchSize;
    this.bulkLoadObjectMapper = bulkLoadObjectMapper;
    this.gcsPdao = gcsPdao;
    this.userRequest = userRequest;
  }

  public IngestPopulateFileStateFromFileStep(
      LoadService loadService,
      int maxBadLoadFileLineErrorsReported,
      int batchSize,
      ObjectMapper bulkLoadObjectMapper) {
    this(
        loadService, maxBadLoadFileLineErrorsReported, batchSize, bulkLoadObjectMapper, null, null);
  }

  void readFile(BufferedReader reader, FlightContext context, CloudPlatformWrapper platform)
      throws IOException {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

    List<String> errorDetails = new ArrayList<>();
    long lineCount = 0;
    List<BulkLoadFileModel> fileList = new ArrayList<>();

    String line;
    while ((line = reader.readLine()) != null) {
      lineCount++;

      try {
        BulkLoadFileModel loadFile = bulkLoadObjectMapper.readValue(line, BulkLoadFileModel.class);
        if (platform.isGcp()) {
          gcsPdao.validateUserCanRead(List.of(loadFile.getSourcePath()), userRequest);
        }
        fileList.add(loadFile);
      } catch (IOException | BlobAccessNotAuthorizedException ex) {
        if (errorDetails.size() < maxBadLoadFileLineErrorsReported) {
          errorDetails.add("Error at line " + lineCount + ": " + ex.getMessage());
        } else {
          errorDetails.add(
              "Error details truncated. [MaxBadLoadFileLineErrorsReported = "
                  + maxBadLoadFileLineErrorsReported
                  + "]");
          throw new BulkLoadControlFileException(
              "Invalid lines in the control file. [All lines in control file must be valid - maxFailedFileLoads field applicable once the control file is validated.]",
              errorDetails);
        }
      }

      // Keep this check and load out of the inner try; it should only catch objectMapper failures
      if (fileList.size() > batchSize) {
        loadService.populateFiles(loadId, fileList);
        fileList.clear();
      }
    }

    // If there are errors in the load file, don't do the load
    if (errorDetails.size() > 0) {
      throw new BulkLoadControlFileException(
          "Invalid lines in the control file. [All lines in control file must be valid in order to proceed - 'maxFailedFileLoads' not applicable here.]",
          errorDetails);
    }

    if (fileList.size() > 0) {
      loadService.populateFiles(loadId, fileList);
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
