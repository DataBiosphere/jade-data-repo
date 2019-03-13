package bio.terra.service;

import bio.terra.controller.exception.ApiException;
import bio.terra.dao.StudyDao;
import bio.terra.controller.exception.ValidationException;
import bio.terra.flight.FlightResponse;
import bio.terra.flight.FlightUtils;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.flight.study.delete.StudyDeleteFlight;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.Stairway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
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
        return studyDao.enumerate(offset, limit)
            .stream()
            .map(summary -> StudyJsonConversion.studySummaryModelFromStudySummary(summary))
            .collect(Collectors.toList());
    }

    public DeleteResponseModel delete(UUID id) {
        List<DatasetSummaryModel> referencedDatasets = datasetService.getDatasetsReferencingStudy(id);
        if (referencedDatasets == null || referencedDatasets.isEmpty()) {
            FlightMap flightMap = new FlightMap();
            flightMap.put(JobMapKeys.REQUEST.getKeyName(), id);
            flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Deleting the study with ID " + id);
            String flightId = stairway.submit(StudyDeleteFlight.class, flightMap);
            return getResponse(flightId, DeleteResponseModel.class);
        } else throw new ValidationException("Can not delete study being used by datasets " + referencedDatasets);
    }

    private <T> T getResponse(String flightId, Class<T> resultClass) {
        stairway.waitForFlight(flightId);
        FlightState result = stairway.getFlightState(flightId);
        FlightResponse flightResponse = FlightUtils.makeFlightResponse(result);
        // TODO: Figure out a better way of returning an ErrorModel rather than re-throwing
        if (flightResponse.isErrorResponse()) {
            ErrorModel errorModel = (ErrorModel) flightResponse.getResponse();
            throw new ApiException(errorModel.getMessage());
        }
        return resultClass.cast(flightResponse.getResponse());
    }
}
