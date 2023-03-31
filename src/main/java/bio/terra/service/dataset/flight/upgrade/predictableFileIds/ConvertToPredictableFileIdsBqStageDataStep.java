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

public class ConvertToPredictableFileIdsBqStageDataStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(ConvertToPredictableFileIdsBqStageDataStep.class);

  private final UUID datasetId;
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;

  public ConvertToPredictableFileIdsBqStageDataStep(
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
    // Load file ids into the table
    bigQueryDatasetPdao.fileIdMappingToStagingTable(dataset, oldToNewMappings);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
