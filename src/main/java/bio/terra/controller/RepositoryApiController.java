package bio.terra.controller;

import bio.terra.dao.StudyDAO;
import bio.terra.flight.StudyCreateFlight;
import bio.terra.metadata.Study;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightResult;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Controller
public class RepositoryApiController implements RepositoryApi {

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final StudyDAO studyDAO;

    @Autowired
    public RepositoryApiController(ObjectMapper objectMapper, HttpServletRequest request, StudyDAO studyDAO) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.studyDAO = studyDAO;
    }

    @Override
    public Optional<ObjectMapper> getObjectMapper() {
        return Optional.ofNullable(objectMapper);
    }

    @Override
    public Optional<HttpServletRequest> getRequest() {
        return Optional.ofNullable(request);
    }

    @Override
    public ResponseEntity<StudySummaryModel> createStudy(@RequestBody StudyRequestModel studyRequest) {
        // TODO: move to stairway service
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Stairway stairway = new Stairway(executorService);

        FlightMap flightMap = new FlightMap();
        flightMap.put("study", new Study(studyRequest));
        flightMap.put("studyDAO", studyDAO);
        String flightId = stairway.submit(StudyCreateFlight.class, flightMap);
        FlightResult result = stairway.getResult(flightId);
        if (result.isSuccess()) {
            FlightMap resultMap = result.getResultMap();
            StudySummaryModel studySummary = resultMap.get("summary", StudySummaryModel.class);
            return new ResponseEntity<>(studySummary, HttpStatus.OK);
        } else {
            Optional<Throwable> throwable = result.getThrowable();
            if (throwable.isPresent()) {
                //....
                Throwable throwableThing = throwable.get();
                throwableThing.printStackTrace();
            }
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
