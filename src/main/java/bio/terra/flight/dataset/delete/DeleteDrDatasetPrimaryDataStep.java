package bio.terra.flight.dataset.delete;

import bio.terra.dao.DrDatasetDao;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.DrDataset;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteDrDatasetPrimaryDataStep implements Step {
    private BigQueryPdao bigQueryPdao;
    private GcsPdao gcsPdao;
    private FireStoreFileDao fileDao;
    private DrDatasetDao datasetDao;

    public DeleteDrDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                      GcsPdao gcsPdao,
                                      FireStoreFileDao fileDao,
                                      DrDatasetDao datasetDao) {
        this.bigQueryPdao = bigQueryPdao;
        this.gcsPdao = gcsPdao;
        this.fileDao = fileDao;
        this.datasetDao = datasetDao;
    }

    DrDataset getDataset(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        UUID datasetId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
        return datasetDao.retrieve(datasetId);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        DrDataset dataset = getDataset(context);
        bigQueryPdao.deleteDataset(dataset);
        gcsPdao.deleteFilesFromDataset(dataset);
        fileDao.deleteFilesFromDataset(dataset.getId().toString());

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
