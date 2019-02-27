package bio.terra.service;

import bio.terra.controller.exception.ApiException;
import bio.terra.dao.StudyDao;
import bio.terra.exceptions.ValidationException;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class StudyService {
    private final StudyDao studyDao;
    private final Stairway stairway;
    private final DatasetService datasetService;

    @Autowired
    public StudyService(StudyDao studyDao, Stairway stairway, DatasetService datasetService) {
        this.studyDao = studyDao;
        this.stairway = stairway;
        this.datasetService = datasetService;
    }

    public StudySummaryModel createStudy(StudyRequestModel studyRequest) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), studyRequest);
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Creating a study");
        String flightId = stairway.submit(StudyCreateFlight.class, flightMap);
        return getResponse(flightId, StudySummaryModel.class);
    }

    public StudyModel retrieve(UUID id) {
        return StudyJsonConversion.studyModelFromStudy(studyDao.retrieve(id));
    }

    public List<StudySummaryModel> enumerate(int offset, int limit) {
        return studyDao.enumerate()
            .stream()
            .map(summary -> StudyJsonConversion.studySummaryModelFromStudySummary(summary))
            .collect(Collectors.toList());
    }

    public boolean delete(UUID id) {
        List<DatasetSummaryModel> referencedDatasets = datasetService.getDatasetsReferencingStudy(id);
        if (referencedDatasets == null || referencedDatasets.isEmpty())
            return studyDao.delete(id);
        else throw new ValidationException("Can not delete study being used by datasets " + referencedDatasets);
    }

    private <T> T getResponse(String flightId, Class<T> resultClass) {
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        if (result.getFlightStatus() == FlightStatus.SUCCESS) {
            if (result.getResultMap().isPresent()) {
                FlightMap resultMap = result.getResultMap().get();
                return resultMap.get(JobMapKeys.RESPONSE.getKeyName(), resultClass);
            }
            // It should not happen that we have success and no result map
            // This is probably not the right exception, but we will replace this with
            // the async stuff and the problem will go away.
            throw new ApiException("Successful job, but no response!");
        }

        String message = "Could not complete flight";
        Optional<String> optErrorMessage = result.getErrorMessage();
        if (optErrorMessage.isPresent()) {
            message = message + ": " + optErrorMessage.get();
        }
        throw new ApiException(message);
    }
}
