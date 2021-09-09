package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.FlightUtils;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsBufferedReader;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import com.google.cloud.storage.Storage;
import java.io.BufferedReader;
import java.util.UUID;

// Populate the files to be loaded from the incoming array
public class IngestPopulateFileStateFromFileGcpStep extends IngestPopulateFileStateFromFileStep {
  private final LoadService loadService;
  private final GcsPdao gcsPdao;

  public IngestPopulateFileStateFromFileGcpStep(
      LoadService loadService, int maxBadLines, int batchSize, GcsPdao gcsPdao) {
    super(loadService, maxBadLines, batchSize);
    this.loadService = loadService;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap inputParameters = context.getInputParameters();
    BulkLoadRequestModel loadRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BulkLoadRequestModel.class);

    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));
    GoogleBucketResource bucketResource =
        FlightUtils.getContextValue(context, FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    Storage storage = gcsPdao.storageForBucket(bucketResource);
    String projectId = bucketResource.projectIdForBucket();
    BufferedReader reader =
        new GcsBufferedReader(storage, projectId, loadRequest.getLoadControlFile());
    readFile(reader, loadId);
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
