package bio.terra.controller;

import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.AsyncService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.Optional;

@Controller
public class RepositoryApiController implements RepositoryApi {

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final AsyncService asyncService;

    @Autowired
    public RepositoryApiController(ObjectMapper objectMapper, HttpServletRequest request, AsyncService asyncService) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.asyncService = asyncService;
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
    public ResponseEntity<StudySummaryModel> createStudy(@Valid @RequestBody StudyRequestModel studyRequest) {
        String jobId = asyncService.submitJob("create-study", studyRequest);
        StudySummaryModel studySummary = asyncService.waitForJob(jobId, StudySummaryModel.class);
        return new ResponseEntity<>(studySummary, HttpStatus.CREATED);
    }
}
