package bio.terra.service.dataset.flight.ingest.scratch;

import static bio.terra.service.dataset.flight.ingest.IngestMapKeys.TABLE_NAME;

import bio.terra.service.common.gcs.CommonFlightKeys;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import com.google.cloud.storage.BlobId;

public class CreateScratchFileForGCPStep implements DefaultUndoStep {
  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String tableName = context.getInputParameters().get(TABLE_NAME, String.class);
    GoogleBucketResource bucket =
        workingMap.get(CommonFlightKeys.SCRATCH_BUCKET_INFO, GoogleBucketResource.class);
    BlobId scratchFilePath =
        GcsUriUtils.getBlobForFlight(
            bucket.getName(), "ingest-scratch/" + tableName, context.getFlightId());

    String path = GcsUriUtils.getGsPathFromBlob(scratchFilePath);

    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), path);
    return StepResult.getStepResultSuccess();
  }
}
