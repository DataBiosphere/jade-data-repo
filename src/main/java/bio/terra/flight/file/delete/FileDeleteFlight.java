package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.UserRequestInfo;
import org.springframework.context.ApplicationContext;

import java.util.Map;
import java.util.UUID;

public class FileDeleteFlight extends Flight {

    public FileDeleteFlight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        super(inputParameters, applicationContext, userRequestInfo);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        FireStoreFileDao fileDao = (FireStoreFileDao)appContext.getBean("fireStoreFileDao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");

        Map<String, String> pathParams = (Map<String, String>) inputParameters.get(
            JobMapKeys.PATH_PARAMETERS.getKeyName(), Map.class);
        String datasetId = pathParams.get(JobMapKeys.DATASET_ID.getKeyName());
        String fileId = pathParams.get(JobMapKeys.FILE_ID.getKeyName());
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
