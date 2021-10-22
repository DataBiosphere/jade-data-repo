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
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.storage.BlobId;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class DataDeletionCopyFilesToBigQueryScratchBucketStep extends SkippableStep {

  private final DatasetService datasetService;
  private final GcsPdao gcsPdao;

  public DataDeletionCopyFilesToBigQueryScratchBucketStep(
      DatasetService datasetService, GcsPdao gcsPdao, Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.datasetService = datasetService;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context)
      throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = getDataset(context, datasetService);
    String projectId = dataset.getProjectResource().getGoogleProjectId();
    DataDeletionRequest dataDeletionRequest = getRequest(context);
    Set<String> tablesNeedingCopy =
        FlightUtils.getTyped(workingMap, DataDeletionMapKeys.TABLE_NAMES_NEEDING_COPY);
    GoogleBucketResource bucketResource =
        FlightUtils.getTyped(workingMap, CommonFlightKeys.SCRATCH_BUCKET_INFO);
    List<DataDeletionTableModel> tables = dataDeletionRequest.getTables();
    for (var table : tables) {
      if (tablesNeedingCopy.contains(table.getTableName())) {
        BlobId from = GcsUriUtils.parseBlobUri(table.getGcsFileSpec().getPath());
        String to =
            GcsUriUtils.getGsPathFromComponents(
                bucketResource.getName(),
                String.format("%s/%s", context.getFlightId(), from.getName()));
        gcsPdao.copyGcsFile(from, GcsUriUtils.parseBlobUri(to), projectId);
        table.getGcsFileSpec().path(to);
      }
    }
    workingMap.put(DataDeletionMapKeys.TABLES, tables);

    return StepResult.getStepResultSuccess();
  }
}
