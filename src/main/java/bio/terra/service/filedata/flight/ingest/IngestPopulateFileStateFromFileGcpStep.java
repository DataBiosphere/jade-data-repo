package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.service.filedata.exception.BulkLoadControlFileException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsBufferedReader;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Storage;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

// Populate the files to be loaded from the incoming array
public class IngestPopulateFileStateFromFileGcpStep extends IngestPopulateFileStateFromFileStep {
  private final GcsPdao gcsPdao;

  public IngestPopulateFileStateFromFileGcpStep(
      LoadService loadService,
      int maxBadLines,
      int batchSize,
      GcsPdao gcsPdao,
      ObjectMapper bulkLoadObjectMapper,
      ExecutorService executor,
      AuthenticatedUserRequest userRequest) {
    super(
        loadService, maxBadLines, batchSize, bulkLoadObjectMapper, gcsPdao, executor, userRequest);
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // Gather vars required to build GcsBufferedReader
    FlightMap inputParameters = context.getInputParameters();
    BulkLoadRequestModel loadRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BulkLoadRequestModel.class);
    GoogleBucketResource bucketResource =
        FlightUtils.getContextValue(context, FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    Storage storage = gcsPdao.storageForBucket(bucketResource);
    String projectId = bucketResource.projectIdForBucket();

    // Stream from control file and build list of files to be ingested
    try (BufferedReader reader =
        new GcsBufferedReader(storage, projectId, loadRequest.getLoadControlFile())) {
      readFile(reader, projectId, context);

    } catch (IOException ex) {
      throw new BulkLoadControlFileException("Failure accessing the load control file in GCS", ex);
    }
    return StepResult.getStepResultSuccess();
  }
}
