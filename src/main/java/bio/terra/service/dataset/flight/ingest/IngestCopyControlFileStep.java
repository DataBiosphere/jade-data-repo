package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.gcs.CommonFlightKeys;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.storage.BlobId;
import java.util.function.Predicate;

public class IngestCopyControlFileStep extends OptionalStep {

  private final DatasetService datasetService;
  private final GcsPdao gcsPdao;

  public IngestCopyControlFileStep(
      DatasetService datasetService, GcsPdao gcsPdao, Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.datasetService = datasetService;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doOptionalStep(FlightContext context)
      throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    GoogleBucketResource bucketResource =
        workingMap.get(CommonFlightKeys.SCRATCH_BUCKET_INFO, GoogleBucketResource.class);
    String projectId = dataset.getProjectResource().getGoogleProjectId();

    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    for (BlobId from : gcsPdao.listGcsIngestBlobs(ingestRequest.getPath(), projectId)) {
      BlobId to =
          GcsUriUtils.getBlobForFlight(
              bucketResource.getName(), from.getName(), context.getFlightId());
      gcsPdao.copyGcsFile(from, to, projectId);
    }

    workingMap.put(
        IngestMapKeys.INGEST_CONTROL_FILE_PATH,
        GcsUriUtils.getGsPathFromComponents(
            bucketResource.getName(), context.getFlightId() + "/*"));
    return StepResult.getStepResultSuccess();
  }
}
