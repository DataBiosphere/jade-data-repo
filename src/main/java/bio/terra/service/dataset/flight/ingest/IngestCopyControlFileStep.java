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

public class IngestCopyControlFileStep extends SkippableStep {

  private final DatasetService datasetService;
  private final GcsPdao gcsPdao;

  public IngestCopyControlFileStep(
      DatasetService datasetService, GcsPdao gcsPdao, Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.datasetService = datasetService;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context)
      throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    GoogleBucketResource bucketResource =
        workingMap.get(CommonFlightKeys.SCRATCH_BUCKET_INFO, GoogleBucketResource.class);

    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    String from = ingestRequest.getPath();
    BlobId blobId = GcsUriUtils.parseBlobUri(from);
    String to =
        GcsUriUtils.getGsPathFromComponents(
            bucketResource.getName(),
            String.format("%s/%s", context.getFlightId(), blobId.getName()));
    gcsPdao.copyGcsFile(from, to, dataset.getProjectResource().getGoogleProjectId());

    workingMap.put(IngestMapKeys.INGEST_CONTROL_FILE_PATH, to);
    return StepResult.getStepResultSuccess();
  }
}
