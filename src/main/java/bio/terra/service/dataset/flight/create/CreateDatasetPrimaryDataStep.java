package bio.terra.service.dataset.flight.create;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class CreateDatasetPrimaryDataStep implements Step {
  private final BigQueryPdao pdao;
  private final DatasetDao datasetDao;

  public CreateDatasetPrimaryDataStep(BigQueryPdao pdao, DatasetDao datasetDao) {
    this.pdao = pdao;
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
    pdao.createDataset(dataset);

    JobMapKeys.STATUS_CODE.put(context.getWorkingMap(), HttpStatus.CREATED);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Dataset dataset = getDataset(context);
    pdao.deleteDataset(dataset);

    return StepResult.getStepResultSuccess();
  }
}
