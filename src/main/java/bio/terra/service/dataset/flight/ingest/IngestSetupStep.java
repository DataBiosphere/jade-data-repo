package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class IngestSetupStep implements Step {
  private Logger logger = LoggerFactory.getLogger(IngestSetupStep.class);

  private DatasetService datasetService;
  private ConfigurationService configService;
  private CloudPlatformWrapper cloudPlatform;

  public IngestSetupStep(
      DatasetService datasetService,
      ConfigurationService configService,
      CloudPlatformWrapper cloudPlatform) {
    this.datasetService = datasetService;
    this.configService = configService;
    this.cloudPlatform = cloudPlatform;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();

    if (configService.testInsertFault(ConfigEnum.TABLE_INGEST_LOCK_CONFLICT_STOP_FAULT)) {
      logger.info("TABLE_INGEST_LOCK_CONFLICT_STOP_FAULT");
      while (!configService.testInsertFault(ConfigEnum.TABLE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT)) {
        logger.info("Sleeping for CONTINUE FAULT");
        TimeUnit.SECONDS.sleep(5);
      }
      logger.info("TABLE_INGEST_LOCK_CONFLICT_CONTINUE_FAULT");
    }

    IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);

    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    IngestUtils.putDatasetName(context, dataset.getName());
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);

    if (cloudPlatform.isGcp()) {
      // We don't actually care about the output here since BQ takes the raw "gs://" string as
      // input.
      // As long as parsing succeeds, we're good to move forward.
      IngestUtils.parseBlobUri(ingestRequestModel.getPath());
      String sgName = DatasetUtils.generateAuxTableName(targetTable, "st");
      IngestUtils.putStagingTableName(context, sgName);
    } else if (cloudPlatform.isAzure()) {
      IngestUtils.validateBlobAzureBlobFileURL(ingestRequestModel.getPath());
      workingMap.put(
          IngestMapKeys.PARQUET_FILE_PATH,
          IngestUtils.getParquetFilePath(targetTable.getName(), context.getFlightId()));
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // Nothing to undo
    return StepResult.getStepResultSuccess();
  }
}
