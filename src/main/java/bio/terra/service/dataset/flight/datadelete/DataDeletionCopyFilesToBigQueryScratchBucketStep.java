package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;

import bio.terra.common.FlightUtils;
import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.common.gcs.CommonFlightKeys;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.cloud.storage.BlobId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataDeletionCopyFilesToBigQueryScratchBucketStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(DataDeletionCopyFilesToBigQueryScratchBucketStep.class);
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

    List<DataDeletionTableModel> tables;
    switch (dataDeletionRequest.getSpecType()) {
      case GCSFILE:
        tables = copyGcsFileSpecPaths(context, dataDeletionRequest, projectId, bucketResource);
        break;
      case JSONARRAY:
        tables = writeJsonArraysToFile(context, dataDeletionRequest, projectId, bucketResource);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + dataDeletionRequest.getSpecType());
    }

    workingMap.put(DataDeletionMapKeys.TABLES, tables);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    // DataDeletionMapKeys.TABLES is not populated until the end of this "DO" step
    // If this step fails part way through, it will be null
    if (FlightUtils.getTyped(workingMap, DataDeletionMapKeys.TABLES) != null) {
      DataDeletionUtils.deleteScratchFiles(context, gcsPdao);
    } else {
      logger.warn(
          "Unable to clean up scratch files because DataDeletionMapKeys.TABLES key was not populated.");
    }
    return StepResult.getStepResultSuccess();
  }

  private List<DataDeletionTableModel> copyGcsFileSpecPaths(
      FlightContext context,
      DataDeletionRequest dataDeletionRequest,
      String projectId,
      GoogleBucketResource bucketResource) {
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
    return tables;
  }

  private List<DataDeletionTableModel> writeJsonArraysToFile(
      FlightContext context,
      DataDeletionRequest dataDeletionRequest,
      String projectId,
      GoogleBucketResource bucketResource) {

    List<DataDeletionTableModel> jsonTables = dataDeletionRequest.getTables();
    List<DataDeletionTableModel> gcsFileTables = new ArrayList<>();
    String flightId = context.getFlightId();

    for (var jsonTable : jsonTables) {
      var scratchFilePath =
          GcsUriUtils.getBlobForFlight(
              bucketResource.getName(), jsonTable.getTableName(), flightId);
      gcsPdao.createGcsFile(scratchFilePath, projectId);

      String path = GcsUriUtils.getGsPathFromBlob(scratchFilePath);
      Stream<String> rowIds = jsonTable.getJsonArraySpec().getRowIds().stream().map(UUID::toString);
      gcsPdao.writeStreamToCloudFile(path, rowIds, projectId);

      var gcsFileTable =
          new DataDeletionTableModel()
              .tableName(jsonTable.getTableName())
              .gcsFileSpec(
                  new DataDeletionGcsFileModel()
                      .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)
                      .path(path));

      gcsFileTables.add(gcsFileTable);
    }
    return gcsFileTables;
  }
}
