package bio.terra.flight.file.delete;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.Study;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.service.StudyService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class FileDeleteFlight extends Flight {

    public FileDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        FireStoreFileDao fileDao = (FireStoreFileDao)appContext.getBean("fireStoreFileDao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");
        StudyService studyService = (StudyService) appContext.getBean("studyService");

        String studyId = inputParameters.get(JobMapKeys.STUDY_ID.getKeyName(), String.class);
        String fileId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), String.class);
        Study study = studyService.retrieve(UUID.fromString(studyId));
        // The flight plan:
        // 1. Metadata start step:
        //    Make sure the file is deletable - not in a dataset
        //    Mark the file as deleting so it is not added to a dataset in the meantime.
        // 2. pdao does the file delete
        // 3. Metadata complete step: delete the file metadata

        // NOTE: The start step may find that the file does not exist. In that case, we still execute the rest
        // of the steps. If the file system data and the bucket storage are out of sync, we can fix it by
        // performing this delete-by-id and it will clean up the bucket or the file system even if they
        // are inconsistent.
        addStep(new DeleteFileMetadataStepStart(fileDao, fileId, study));
        addStep(new DeleteFilePrimaryDataStep(study, fileId, gcsPdao));
        addStep(new DeleteFileMetadataStepComplete(fileDao, fileId, study));
    }

}
