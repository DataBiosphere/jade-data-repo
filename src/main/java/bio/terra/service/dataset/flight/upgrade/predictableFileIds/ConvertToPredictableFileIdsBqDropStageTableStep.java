package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertToPredictableFileIdsBqDropStageTableStep extends DefaultUndoStep {
  private static final Logger logger =
      LoggerFactory.getLogger(ConvertToPredictableFileIdsBqDropStageTableStep.class);

  private final UUID datasetId;
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;

  public ConvertToPredictableFileIdsBqDropStageTableStep(
      UUID datasetId, DatasetService datasetService, BigQueryDatasetPdao bigQueryDatasetPdao) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);

    // Drop the table
    bigQueryDatasetPdao.deleteStagingFileIdMappingTable(dataset);

    return StepResult.getStepResultSuccess();
  }
}
