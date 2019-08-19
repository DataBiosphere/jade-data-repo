package bio.terra.flight.dataset.delete;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.service.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteDatasetPrimaryDataStep implements Step {
    private BigQueryPdao bigQueryPdao;
    private GcsPdao gcsPdao;
    private FireStoreFileDao fileDao;
    private DatasetService datasetService;
    private UUID datasetId;

    public DeleteDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                        GcsPdao gcsPdao,
                                        FireStoreFileDao fileDao,
                                        DatasetService datasetService,
                                        UUID datasetId) {
        this.bigQueryPdao = bigQueryPdao;
        this.gcsPdao = gcsPdao;
        this.fileDao = fileDao;
        this.datasetService = datasetService;
        this.datasetId = datasetId;
    }

    Dataset getDataset(FlightContext context) {
        return datasetService.retrieve(datasetId);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = getDataset(context);
        bigQueryPdao.deleteDataset(dataset);
        gcsPdao.deleteFilesFromDataset(dataset);
        fileDao.deleteFilesFromDataset(dataset);

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
