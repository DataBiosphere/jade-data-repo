package bio.terra.service.dataset.flight.delete;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteDatasetPrimaryDataStep implements Step {
    private BigQueryPdao bigQueryPdao;
    private GcsPdao gcsPdao;
    private FireStoreDao fileDao;
    private DatasetService datasetService;
    private UUID datasetId;

    public DeleteDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                        GcsPdao gcsPdao,
                                        FireStoreDao fileDao,
                                        DatasetService datasetService,
                                        UUID datasetId) {
        this.bigQueryPdao = bigQueryPdao;
        this.gcsPdao = gcsPdao;
        this.fileDao = fileDao;
        this.datasetService = datasetService;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = datasetService.retrieve(datasetId);
        bigQueryPdao.deleteDataset(dataset);
        fileDao.deleteFilesFromDataset(dataset, fireStoreFile -> gcsPdao.deleteFile(fireStoreFile));
        FlightMap map = context.getWorkingMap();
        map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.NO_CONTENT);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // can't undo delete
        return StepResult.getStepResultSuccess();
    }
}
