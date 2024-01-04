package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class CreateDatasetPrimaryDataStep implements Step {
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final DatasetDao datasetDao;

  public CreateDatasetPrimaryDataStep(
      BigQueryDatasetPdao bigQueryDatasetPdao, DatasetDao datasetDao) {
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.datasetDao = datasetDao;
  }

  private Dataset getDataset(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    return datasetDao.retrieve(datasetId);
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = getDataset(context);
    bigQueryDatasetPdao.createDataset(dataset);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Dataset dataset = getDataset(context);
    bigQueryDatasetPdao.deleteDataset(dataset);

    return StepResult.getStepResultSuccess();
  }
}
