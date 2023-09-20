package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;

/**
 * The setup step required to generate the staging file name.
 *
 * <p>You might ask, "why can't you do that in the staging table step?" The answer is that we need a
 * step boundary so that the staging table name is written to the database. Otherwise, on a failure
 * of stairway, the staging table name would be lost and could not be found to either continue the
 * ingest or to undo it.
 *
 * <p>In addition, it sets up several other things:
 *
 * <p>First, it does an existence check on the source blob to ensure it is accessible to the data
 * repository. We could put this off and only do it in the load step. I am thinking that it is
 * better to do a sanity check before we create objects in BigQuery. It is no guarantee of course,
 * since the file could be deleted by the time we try the load.
 *
 * <p>Second, it stores away the dataset name. Several steps only need the dataset name and not the
 * dataset object.
 */
public class IngestSetupStep extends DefaultUndoStep {

  private DatasetService datasetService;
  private CloudPlatformWrapper cloudPlatform;

  public IngestSetupStep(DatasetService datasetService, CloudPlatformWrapper cloudPlatform) {
    this.datasetService = datasetService;
    this.cloudPlatform = cloudPlatform;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);

    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    IngestUtils.putDatasetName(context, dataset.getName());
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);

    if (cloudPlatform.isGcp()) {
      GcsUriUtils.validateBlobUri(ingestRequestModel.getPath());
      String sgName = DatasetUtils.generateAuxTableName(targetTable, "st");
      IngestUtils.putStagingTableName(context, sgName);
    } else if (cloudPlatform.isAzure()) {
      // Don't validate if we are ingesting as a payload object
      if (!IngestUtils.isIngestFromPayload(context.getInputParameters())) {
        IngestUtils.validateBlobAzureBlobFileURL(ingestRequestModel.getPath());
      }
      workingMap.put(
          IngestMapKeys.PARQUET_FILE_PATH,
          IngestUtils.getParquetFilePath(targetTable.getName(), context.getFlightId()));
    }

    return StepResult.getStepResultSuccess();
  }
}
