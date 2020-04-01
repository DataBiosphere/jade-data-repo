package bio.terra.service.dataset.flight.delete;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
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
    private ConfigurationService configService;

    public DeleteDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                        GcsPdao gcsPdao,
                                        FireStoreDao fileDao,
                                        DatasetService datasetService,
                                        UUID datasetId,
                                        ConfigurationService configService) {
        this.bigQueryPdao = bigQueryPdao;
        this.gcsPdao = gcsPdao;
        this.fileDao = fileDao;
        this.datasetService = datasetService;
        this.datasetId = datasetId;
        this.configService = configService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = datasetService.retrieve(datasetId);
        bigQueryPdao.deleteDataset(dataset);
        if (configService.testInsertFault(ConfigEnum.LOAD_SKIP_FILE_LOAD)) {
            // If we didn't load files, don't try to delete them
            fileDao.deleteFilesFromDataset(dataset, fireStoreFile -> { });
        } else {
            fileDao.deleteFilesFromDataset(dataset, fireStoreFile -> gcsPdao.deleteFile(fireStoreFile));
        }

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
