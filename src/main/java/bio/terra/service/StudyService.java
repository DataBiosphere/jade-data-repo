package bio.terra.service;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dao.StudyDao;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.flight.study.delete.StudyDeleteFlight;
import bio.terra.flight.study.ingest.IngestMapKeys;
import bio.terra.flight.study.ingest.StudyIngestFlight;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.metadata.StudySummary;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateStudyModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
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
    private final JobService jobService; // for handling flight response

    @Autowired
    public StudyService(StudyDao studyDao, Stairway stairway, JobService jobService) {
        this.studyDao = studyDao;
        this.stairway = stairway;
        this.jobService = jobService;
    }

    public StudySummaryModel createStudy(StudyRequestModel studyRequest, AuthenticatedUserRequest userInfo) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), studyRequest);
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Creating a study");
        flightMap.put(JobMapKeys.USER_INFO.getKeyName(), userInfo);
        String flightId = stairway.submit(StudyCreateFlight.class, flightMap);
        return getResponse(flightId, StudySummaryModel.class);
    }

    public StudyModel retrieve(UUID id) {
        return StudyJsonConversion.studyModelFromStudy(studyDao.retrieve(id));
    }

    
    public EnumerateStudyModel enumerate(
        int offset, int limit, String sort, String direction, String filter, List<ResourceAndAccessPolicy> resources) {
        if (resources.isEmpty()) {
            return new EnumerateStudyModel().total(0);
        }
        List<UUID> resourceIds = resources
            .stream()
            .map(resource -> UUID.fromString(resource.getResourceId()))
            .collect(Collectors.toList());
        MetadataEnumeration<StudySummary> studyEnum = studyDao.enumerate(
            offset, limit, sort, direction, filter, resourceIds);
        List<StudySummaryModel> summaries = studyEnum.getItems()
            .stream()
            .map(summary -> StudyJsonConversion.studySummaryModelFromStudySummary(summary))
            .collect(Collectors.toList());
        return new EnumerateStudyModel().items(summaries).total(studyEnum.getTotal());
    }

    public DeleteResponseModel delete(UUID id, AuthenticatedUserRequest userInfo) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), id);
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Deleting the study with ID " + id);
        flightMap.put(JobMapKeys.USER_INFO.getKeyName(), userInfo);
        String flightId = stairway.submit(StudyDeleteFlight.class, flightMap);
        return getResponse(flightId, DeleteResponseModel.class);
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
