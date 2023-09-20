package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.common.MapUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertToPredictableFileIdsBqStageDataStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(ConvertToPredictableFileIdsBqStageDataStep.class);

  private static final int INSERT_CHUNK_SIZE = 1000;

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
      // Note: this situation might arise if either the file ids were already converted or if there
      // are no fileids in this particular dataset
      logger.info("No file ids to migrate");
      return StepResult.getStepResultSuccess();
    }
    // Load file ids into the table
    // Chunk the writes to avoid overwhelming BigQuery
    List<Map<UUID, UUID>> chunkedOldToNewMappings =
        MapUtils.partitionMap(oldToNewMappings, INSERT_CHUNK_SIZE);

    int i = 0;
    for (Map<UUID, UUID> chunk : chunkedOldToNewMappings) {
      logger.info("inserting chunk {} of {}", ++i, chunkedOldToNewMappings.size());
      bigQueryDatasetPdao.fileIdMappingToStagingTable(dataset, chunk);
    }

    return StepResult.getStepResultSuccess();
  }
}
