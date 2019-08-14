package bio.terra.flight.file.delete;

import bio.terra.controller.UserInfo;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.service.DatasetService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class FileDeleteFlight extends Flight {

    public FileDeleteFlight(FlightMap inputParameters, Object applicationContext, UserInfo userInfo) {
        super(inputParameters, applicationContext, userInfo);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        FireStoreFileDao fileDao = (FireStoreFileDao)appContext.getBean("fireStoreFileDao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        String datasetId = inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        String fileId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), String.class);
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        // The flight plan:
        // 1. Metadata start step:
        //    Make sure the file is deletable - not in a snapshot
        //    Mark the file as deleting so it is not added to a snapshot in the meantime.
        // 2. pdao does the file delete
        // 3. Metadata complete step: delete the file metadata

        // NOTE: The start step may find that the file does not exist. In that case, we still execute the rest
        // of the steps. If the file system data and the bucket storage are out of sync, we can fix it by
        // performing this delete-by-id and it will clean up the bucket or the file system even if they
        // are inconsistent.
        addStep(new DeleteFileMetadataStepStart(fileDao, fileId, dataset));
        addStep(new DeleteFilePrimaryDataStep(dataset, fileId, gcsPdao, fileDao));
        addStep(new DeleteFileMetadataStepComplete(fileDao, fileId, dataset));
    }

}
