package bio.terra.service;

import bio.terra.controller.exception.ValidationException;
import bio.terra.dao.StudyDao;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.flight.study.delete.StudyDeleteFlight;
import bio.terra.flight.study.ingest.IngestMapKeys;
import bio.terra.flight.study.ingest.StudyIngestFlight;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.model.UserInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class StudyService {
    private final StudyDao studyDao;
    private final Stairway stairway;
    private final DatasetService datasetService;
    private final JobService jobService; // for handling flight response

    @Autowired
    public StudyService(StudyDao studyDao, Stairway stairway, DatasetService datasetService, JobService jobService) {
        this.studyDao = studyDao;
        this.stairway = stairway;
        this.datasetService = datasetService;
        this.jobService = jobService;
    }

    public StudySummaryModel createStudy(StudyRequestModel studyRequest, String token) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), studyRequest);
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Creating a study");
        flightMap.put(JobMapKeys.TOKEN.getKeyName(), token);
        flightMap.put(JobMapKeys.USER_INFO.getKeyName(), new UserInfo().userEmail("ahaesslydev@gmail.com"));
        String flightId = stairway.submit(StudyCreateFlight.class, flightMap);
        return getResponse(flightId, StudySummaryModel.class);
    }

    public StudyModel retrieve(UUID id) {
        return StudyJsonConversion.studyModelFromStudy(studyDao.retrieve(id));
    }

    public List<StudySummaryModel> enumerate(int offset, int limit) {
        return studyDao.enumerate(offset, limit)
            .stream()
            .map(summary -> StudyJsonConversion.studySummaryModelFromStudySummary(summary))
            .collect(Collectors.toList());
    }

    public DeleteResponseModel delete(UUID id, String token) {
        List<DatasetSummaryModel> referencedDatasets = datasetService.getDatasetsReferencingStudy(id);
        if (referencedDatasets == null || referencedDatasets.isEmpty()) {
            FlightMap flightMap = new FlightMap();
            flightMap.put(JobMapKeys.REQUEST.getKeyName(), id);
            flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Deleting the study with ID " + id);
            flightMap.put(JobMapKeys.TOKEN.getKeyName(), token);
            flightMap.put(JobMapKeys.USER_INFO.getKeyName(), new UserInfo().userEmail("ahaesslydev@gmail.com"));
            String flightId = stairway.submit(StudyDeleteFlight.class, flightMap);
            return getResponse(flightId, DeleteResponseModel.class);
        } else throw new ValidationException("Can not delete study being used by datasets " + referencedDatasets);
    }

    public String ingestStudy(String id, IngestRequestModel ingestRequestModel) {
        // Fill in a default load id if the caller did not provide one in the ingest request.
        if (StringUtils.isEmpty(ingestRequestModel.getLoadTag())) {
            String loadTag = "load-at-" + Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT);
            ingestRequestModel.setLoadTag(loadTag);
        }

        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(),
            "Ingest from " + ingestRequestModel.getPath() +
                " to " + ingestRequestModel.getTable() +
                " in study id " + id);
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), ingestRequestModel);
        flightMap.put(IngestMapKeys.STUDY_ID, id);
        return stairway.submit(StudyIngestFlight.class, flightMap);
    }

    private <T> T getResponse(String flightId, Class<T> resultClass) {
        stairway.waitForFlight(flightId);
        return jobService.retrieveJobResult(flightId, resultClass);
    }
}
