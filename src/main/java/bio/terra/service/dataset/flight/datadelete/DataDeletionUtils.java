package bio.terra.service.dataset.flight.datadelete;

import bio.terra.common.FlightUtils;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.common.gcs.CommonFlightKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.List;
import java.util.UUID;

public final class DataDeletionUtils {

  private DataDeletionUtils() {}

  public static DataDeletionRequest getRequest(FlightContext context) {
    return context
        .getInputParameters()
        .get(JobMapKeys.REQUEST.getKeyName(), DataDeletionRequest.class);
  }

  public static Dataset getDataset(FlightContext context, DatasetService datasetService) {
    String datasetId =
        context.getInputParameters().get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
    return datasetService.retrieve(UUID.fromString(datasetId));
  }

  public static void deleteScratchFiles(FlightContext context, GcsPdao gcsPdao) {
    FlightMap workingMap = context.getWorkingMap();
    GoogleBucketResource bucketResource =
        FlightUtils.getTyped(workingMap, CommonFlightKeys.SCRATCH_BUCKET_INFO);
    List<DataDeletionTableModel> tables =
        FlightUtils.getTyped(workingMap, DataDeletionMapKeys.TABLES);
    for (var table : tables) {
      var path = table.getGcsFileSpec().getPath();
      gcsPdao.deleteFileByGspath(path, bucketResource.projectIdForBucket());
    }
  }
}
