package bio.terra.controller;

import bio.terra.model.*;
import bio.terra.service.AsyncException;
import bio.terra.service.AsyncService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.HashSet;
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

    @ExceptionHandler(AsyncException.class)
    public ResponseEntity<ErrorModel> handleAsyncException(AsyncException ex) {
        return new ResponseEntity<>(new ErrorModel().message(ex.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    public ResponseEntity<StudySummaryModel> createStudy(@Valid @RequestBody StudyRequestModel studyRequest) {
        // TODO: validation should happen at some point, either at job submission or when the @Valid annotation is used
        HashSet<String> seenTableNames = new HashSet<>();
        for (TableModel table : studyRequest.getSchema().getTables()) {
            String tableName = table.getName();
            if (seenTableNames.contains(tableName)) {
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }
            seenTableNames.add(tableName);
            HashSet<String> seenColumnNames = new HashSet<>();
            for (ColumnModel column : table.getColumns()) {
                String columnName = column.getName();
                if (seenColumnNames.contains(columnName)) {
                    return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
                }
                seenColumnNames.add(columnName);
            }
        }
        String jobId = asyncService.submitJob("create-study", studyRequest);
        StudySummaryModel studySummary = asyncService.waitForJob(jobId, StudySummaryModel.class);
        return new ResponseEntity<>(studySummary, HttpStatus.CREATED);
    }
}
