package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;

import bio.terra.common.FlightUtils;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.common.gcs.CommonFlightKeys;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.BaseStep;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.storage.BlobId;
import java.util.List;
import java.util.Set;

public class DataDeletionCopyFilesToBigQueryScratchBucketStep extends BaseStep {

  private final DatasetService datasetService;
  private final GcsPdao gcsPdao;

  public DataDeletionCopyFilesToBigQueryScratchBucketStep(
      DatasetService datasetService, GcsPdao gcsPdao) {
    this.datasetService = datasetService;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = getDataset(context, datasetService);
    String projectId = dataset.getProjectResource().getGoogleProjectId();
    DataDeletionRequest dataDeletionRequest = getRequest(context);
    GoogleBucketResource bucketResource =
        FlightUtils.getTyped(workingMap, CommonFlightKeys.SCRATCH_BUCKET_INFO);
    List<DataDeletionTableModel> tables = dataDeletionRequest.getTables();
    for (var table : tables) {
      String tablePath = table.getGcsFileSpec().getPath();
      for (BlobId from : gcsPdao.listGcsIngestBlobs(tablePath, projectId)) {
        BlobId to =
            GcsUriUtils.getBlobForFlight(
                bucketResource.getName(), from.getName(), context.getFlightId());
        gcsPdao.copyGcsFile(from, to, projectId);
      }
      String newPath = GcsUriUtils.getControlPath(tablePath, bucketResource, context.getFlightId());
      table.getGcsFileSpec().path(newPath);
    }
    workingMap.put(DataDeletionMapKeys.TABLES, tables);

    return StepResult.getStepResultSuccess();
  }
}
