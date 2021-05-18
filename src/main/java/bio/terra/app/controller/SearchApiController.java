package bio.terra.app.controller;

import bio.terra.controller.SearchApi;
import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.model.SearchQueryRequest;
import bio.terra.model.SearchQueryResultModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.AuthenticatedUserRequestFactory;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.search.SearchService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.util.Optional;
import java.util.UUID;

@Controller
@Api(tags = {"search"})
@ConditionalOnProperty(name = "features.search.api", havingValue = "enabled")
public class SearchApiController implements SearchApi {

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final IamService iamService;
    private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
    private final SearchService searchService;
    private final SnapshotService snapshotService;

    @Autowired
    public SearchApiController(
        ObjectMapper objectMapper,
        HttpServletRequest request,
        IamService iamService,
        AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
        SearchService searchService,
        SnapshotService snapshotService
    ) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.iamService = iamService;
        this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
        this.searchService = searchService;
        this.snapshotService = snapshotService;
    }

    @Override
    public Optional<ObjectMapper> getObjectMapper() {
        return Optional.ofNullable(objectMapper);
    }

    @Override
    public Optional<HttpServletRequest> getRequest() {
        return Optional.ofNullable(request);
    }

    private AuthenticatedUserRequest getAuthenticatedInfo() {
        return authenticatedUserRequestFactory.from(request);
    }

    @Override
    public ResponseEntity<SearchIndexModel> createSearchIndex(
        @PathVariable("id") String id,
        @Valid @RequestBody SearchIndexRequest searchIndexRequest
    ) {
        iamService.verifyAuthorization(getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT, id, IamAction.READ_DATA);
        Snapshot snapshot = snapshotService.retrieve(UUID.fromString(id));
        SearchIndexModel searchIndexModel = searchService.indexSnapshot(snapshot, searchIndexRequest);
        return new ResponseEntity<>(searchIndexModel, HttpStatus.OK);
    }

    @Override
    public ResponseEntity<SearchQueryResultModel> querySearchIndices(
        @Valid @RequestBody SearchQueryRequest searchQueryRequest,
        @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
        @Valid @RequestParam(value = "limit", required = false, defaultValue = "1000") Integer limit
    ) {
        SearchQueryResultModel searchQueryResultModel = new SearchQueryResultModel();
        return new ResponseEntity<>(searchQueryResultModel, HttpStatus.OK);
    }
}
