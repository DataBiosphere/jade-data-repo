package bio.terra.flight.file.ingest;

import bio.terra.dao.FileDao;
import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.service.FileService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

public class FileIngestFlight extends Flight {

    public FileIngestFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        StudyDao studyDao = (StudyDao)appContext.getBean("studyDao");
        FileDao fileDao = (FileDao)appContext.getBean("fileDao");
        FileService fileService = (FileService)appContext.getBean("fileService");

        String studyId = inputParameters.get(JobMapKeys.STUDY_ID.getKeyName(), String.class);
        Study study = studyDao.retrieve(UUID.fromString(studyId));

        // The flight plan:
        // 1. Metadata step:
        //    Compute the target gspath where the file should go
        //    Create the file object in the database; marked as not present
        // 2. pdao does the file copy and returns file gspath, checksum and size
        // 3. Update the file object with the gspath, checksum and size and mark as present
        addStep(new IngestFileMetadataStep(fileDao, study, fileService));
    }

}
