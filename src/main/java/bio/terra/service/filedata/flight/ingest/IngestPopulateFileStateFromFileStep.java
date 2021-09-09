package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.filedata.exception.BulkLoadControlFileException;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// Populate the files to be loaded from the incoming array
public abstract class IngestPopulateFileStateFromFileStep implements Step {
  private final LoadService loadService;
  private final int maxBadLines;
  private final int batchSize;

  public IngestPopulateFileStateFromFileStep(
      LoadService loadService, int maxBadLines, int batchSize) {
    this.loadService = loadService;
    this.maxBadLines = maxBadLines;
    this.batchSize = batchSize;
  }

  private ObjectMapper getObjectMapper() {
    // Ensure that file ingestion works with extra key-value pairs
    return new ObjectMapper()
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }

  public void readFile(BufferedReader reader, UUID loadId) {
    ObjectMapper objectMapper = getObjectMapper();
    List<String> errorDetails = new ArrayList<>();
    try (reader) {
      long lineCount = 0;
      List<BulkLoadFileModel> fileList = new ArrayList<>();

      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        lineCount++;

        try {
          BulkLoadFileModel loadFile = objectMapper.readValue(line, BulkLoadFileModel.class);
          fileList.add(loadFile);
        } catch (IOException ex) {
          errorDetails.add("Format error at line " + lineCount + ": " + ex.getMessage());
          if (errorDetails.size() > maxBadLines) {
            throw new BulkLoadControlFileException(
                "More than " + maxBadLines + " bad lines in the control file", errorDetails);
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
            "There were " + errorDetails.size() + " bad lines in the control file", errorDetails);
      }

      if (fileList.size() > 0) {
        loadService.populateFiles(loadId, fileList);
      }

    } catch (IOException ex) {
      throw new BulkLoadControlFileException("Failure accessing the load control file", ex);
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
