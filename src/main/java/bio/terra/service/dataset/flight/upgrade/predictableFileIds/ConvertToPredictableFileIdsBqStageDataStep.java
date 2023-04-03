package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record ConvertToPredictableFileIdsBqStageDataStep(
    UUID datasetId, DatasetService datasetService, BigQueryDatasetPdao bigQueryDatasetPdao)
    implements DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(ConvertToPredictableFileIdsBqStageDataStep.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);
    Map<UUID, UUID> oldToNewMappings =
        ConvertFileIdUtils.readFlightMappings(context.getWorkingMap());

    if (oldToNewMappings.isEmpty()) {
      // Note: this situation might arise if either the file ids were already converted or if there
      // are no fileids in this particular dataset
      logger.info("No file ids to migrate");
      return StepResult.getStepResultSuccess();
    }
    // Load file ids into the table
    bigQueryDatasetPdao.fileIdMappingToStagingTable(dataset, oldToNewMappings);

    return StepResult.getStepResultSuccess();
  }
}
