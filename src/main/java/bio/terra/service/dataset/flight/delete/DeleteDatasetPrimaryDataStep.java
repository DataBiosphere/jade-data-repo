package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DeleteDatasetPrimaryDataStep extends DefaultUndoStep {

  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final GcsPdao gcsPdao;
  private final FireStoreDao fileDao;
  private final DatasetService datasetService;
  private final UUID datasetId;

  public DeleteDatasetPrimaryDataStep(
      BigQueryDatasetPdao bigQueryDatasetPdao,
      GcsPdao gcsPdao,
      FireStoreDao fileDao,
      DatasetService datasetService,
      UUID datasetId) {
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.gcsPdao = gcsPdao;
    this.fileDao = fileDao;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    bigQueryDatasetPdao.deleteDataset(dataset);
    fileDao.deleteFilesFromDataset(dataset, gcsPdao::deleteFile);

    FlightMap map = context.getWorkingMap();
    map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.NO_CONTENT);
    return StepResult.getStepResultSuccess();
  }
}
