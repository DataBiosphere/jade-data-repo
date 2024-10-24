package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertToPredictableFileIdsBqCreateStageTableStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(ConvertToPredictableFileIdsBqCreateStageTableStep.class);

  private final UUID datasetId;
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;

  public ConvertToPredictableFileIdsBqCreateStageTableStep(
      UUID datasetId, DatasetService datasetService, BigQueryDatasetPdao bigQueryDatasetPdao) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);

    Map<UUID, UUID> oldToNewMappings =
        ConvertFileIdUtils.readFlightMappings(context.getWorkingMap());

    if (oldToNewMappings.isEmpty()) {
      logger.info("No file ids to migrate");
      return StepResult.getStepResultSuccess();
    }

    // Create the table
    bigQueryDatasetPdao.createStagingFileIdMappingTable(dataset);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    Map<UUID, UUID> oldToNewMappings =
        ConvertFileIdUtils.readFlightMappings(context.getWorkingMap());

    if (oldToNewMappings.isEmpty()) {
      logger.info("No file ids to migrate");
      return StepResult.getStepResultSuccess();
    }

    // Delete the table
    bigQueryDatasetPdao.deleteStagingFileIdMappingTable(dataset);

    return StepResult.getStepResultSuccess();
  }
}
