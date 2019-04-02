package bio.terra.flight.file.ingest;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;

public class FileIngestFlight extends Flight {

    public FileIngestFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

/*
COMMENTED OUT SO IT WOULD PASS FINDBUGS - work in progress

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        StudyDao studyDao = (StudyDao)appContext.getBean("studyDao");
        FileDao fileDao = (FileDao)appContext.getBean("fileDao");

        String studyId = inputParameters.get(JobMapKeys.STUDY_ID.getKeyName(), String.class);
        Study study = studyDao.retrieve(UUID.fromString(studyId));
 */
        // <<< YOU ARE HERE >>>

        // addStep(new IngestSetupStep(studyDao));
        // Steps:
        // 1. Test existence of target path; error on dup
        // 2. Find or create directory path and file object - marked as being ingested - get the id
        // 3. Copy the file into the bucket - named by the id
        // 4. update the file object marked as present

    }

}
