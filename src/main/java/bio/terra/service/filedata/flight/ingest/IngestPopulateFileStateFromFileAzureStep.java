package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.FlightUtils;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.service.filedata.exception.BulkLoadControlFileException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsBufferedReader;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.storage.Storage;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// Populate the files to be loaded from the incoming array
public class IngestPopulateFileStateFromFileAzureStep extends IngestPopulateFileStateFromFileStep {
  private final LoadService loadService;
  private final int maxBadLines;
  private final int batchSize;
  private final GcsPdao gcsPdao;

  public IngestPopulateFileStateFromFileAzureStep(
      LoadService loadService, int maxBadLines, int batchSize, GcsPdao gcsPdao) {
    super(loadService, maxBadLines, batchSize, gcsPdao);
    this.loadService = loadService;
    this.maxBadLines = maxBadLines;
    this.batchSize = batchSize;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // Ensure that file ingestion works with extra key-value pairs
    ObjectMapper objectMapper =
        new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    FlightMap inputParameters = context.getInputParameters();
    BulkLoadRequestModel loadRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BulkLoadRequestModel.class);

    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));
    GoogleBucketResource bucketResource =
        FlightUtils.getContextValue(context, FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    Storage storage = gcsPdao.storageForBucket(bucketResource);
    String projectId = bucketResource.projectIdForBucket();
    List<String> errorDetails = new ArrayList<>();

    try (BufferedReader reader =
        new GcsBufferedReader(storage, projectId, loadRequest.getLoadControlFile())) {
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

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

    loadService.cleanFiles(loadId);
    return StepResult.getStepResultSuccess();
  }
}
