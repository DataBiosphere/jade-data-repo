package bio.terra.flight.file.delete;

import bio.terra.dao.StudyDao;
import bio.terra.filesystem.FileDao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class FileDeleteFlight extends Flight {

    public FileDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        StudyDao studyDao = (StudyDao)appContext.getBean("studyDao");
        FileDao fileDao = (FileDao)appContext.getBean("fileDao");
        GcsPdao gcsPdao = (GcsPdao)appContext.getBean("gcsPdao");

        String studyId = inputParameters.get(JobMapKeys.STUDY_ID.getKeyName(), String.class);
        String fileId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), String.class);

        // The flight plan:
        // 1. Metadata start step:
        //    Make sure the file is deletable - not in a dataset
        //    Mark the file as deleting so it is not added to a dataset in the meantime.
        // 2. pdao does the file delete
        // 3. Metadata complete step: delete the file metadata

        // TODO REVIEWERS: One choice I made that I'd like opinions on. The start step may find that
        // the file does not exist. In that case, I still execute the rest of the steps. I could put the
        // existence state into the working map and have the other steps skip doing anything. My thought
        // is that if the file system data and the bucket storage are out of sync, we can fix it by
        // performing this delete-by-id and it will clean up the bucket or the file system even if they
        // are inconsistent.
        addStep(new DeleteFileMetadataStepStart(fileDao, fileId));
        addStep(new DeleteFilePrimaryDataStep(studyDao, studyId, fileId, gcsPdao));
        addStep(new DeleteFileMetadataStepComplete(fileDao, fileId));
    }

}
