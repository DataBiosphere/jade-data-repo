package bio.terra.app.controller;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.controller.exception.ApiException;
import bio.terra.controller.SearchApi;
import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.model.SearchMetadataModel;
import bio.terra.model.SearchQueryRequest;
import bio.terra.model.SearchQueryResultModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.AuthenticatedUserRequestFactory;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.iam.exception.IamForbiddenException;
import bio.terra.service.search.SearchService;
import bio.terra.service.search.SnapshotSearchMetadataDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
@Api(tags = {"search"})
@ConditionalOnProperty(name = "features.search.api", havingValue = "enabled")
public class SearchApiController implements SearchApi {

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final ApplicationConfiguration appConfig;
  private final IamService iamService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final SearchService searchService;
  private final SnapshotService snapshotService;
  private final SnapshotSearchMetadataDao snapshotSearchMetadataDao;

  @Autowired
  public SearchApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      ApplicationConfiguration appConfig,
      IamService iamService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      SearchService searchService,
      SnapshotService snapshotService,
      SnapshotSearchMetadataDao snapshotSearchMetadataDao) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.appConfig = appConfig;
    this.iamService = iamService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.searchService = searchService;
    this.snapshotService = snapshotService;
    this.snapshotSearchMetadataDao = snapshotSearchMetadataDao;
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.of(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }

  @Override
  public ResponseEntity<Object> enumerateSnapshotSearch() {
    List<UUID> authorizedResources =
        iamService.listAuthorizedResources(getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT);
    Map<UUID, String> metadata = searchService.enumerateSnapshotSearch(authorizedResources);
    return ResponseEntity.ok(metadata.values());
  }

  @Override
  public ResponseEntity<SearchIndexModel> createSearchIndex(
      @PathVariable("id") String id, @Valid @RequestBody SearchIndexRequest searchIndexRequest) {
    AuthenticatedUserRequest user = getAuthenticatedInfo();
    // Only admins have the configure action, so effectively this locks the indexing endpoint to
    // admins only
    iamService.verifyAuthorization(
        user, IamResourceType.DATAREPO, appConfig.getResourceId(), IamAction.CONFIGURE);
    iamService.verifyAuthorization(user, IamResourceType.DATASNAPSHOT, id, IamAction.READ_DATA);
    Snapshot snapshot = snapshotService.retrieve(UUID.fromString(id));
    try {
      SearchIndexModel searchIndexModel = searchService.indexSnapshot(snapshot, searchIndexRequest);
      return new ResponseEntity<>(searchIndexModel, HttpStatus.OK);
    } catch (InterruptedException e) {
      throw new ApiException("Could not generate index for snapshot " + id, e);
    }
  }

  @Override
  public ResponseEntity<SearchMetadataModel> upsertSearchMetadata(UUID id, String body) {
    try {
      var user = getAuthenticatedInfo();
      iamService.verifyAuthorization(
          user, IamResourceType.DATASNAPSHOT, id.toString(), IamAction.UPDATE_SNAPSHOT);
      snapshotSearchMetadataDao.putMetadata(id, body);
      SearchMetadataModel searchMetadataModel = new SearchMetadataModel();
      searchMetadataModel.setMetadataSummary("Upserted search metadata for snapshot " + id);
      return ResponseEntity.ok(searchMetadataModel);
    } catch (Exception e) {
      throw new ApiException("Could not upsert metadata for snapshot " + id, e);
    }
  }

  @Override
  public ResponseEntity<Void> deleteSearchMetadata(UUID id) {
    try {
      var user = getAuthenticatedInfo();
      iamService.verifyAuthorization(
          user, IamResourceType.DATASNAPSHOT, id.toString(), IamAction.UPDATE_SNAPSHOT);
      snapshotSearchMetadataDao.deleteMetadata(id);
      return ResponseEntity.noContent().build();
    } catch (Exception e) {
      throw new ApiException("Could not delete metadata for snapshot " + id, e);
    }
  }

  @Override
  public ResponseEntity<SearchQueryResultModel> querySearchIndices(
      SearchQueryRequest searchQueryRequest, Integer offset, Integer limit) {

    List<UUID> accessibleIds =
        iamService.listAuthorizedResources(getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT);

    final List<UUID> snapshotIds = searchQueryRequest.getSnapshotIds();

    Set<UUID> requestIds =
        snapshotIds == null || snapshotIds.isEmpty() ? Set.of() : Set.copyOf(snapshotIds);

    Set<UUID> inaccessibleIds = new HashSet<>(requestIds);
    accessibleIds.forEach(inaccessibleIds::remove);
    if (!inaccessibleIds.isEmpty()) {
      throw new IamForbiddenException(
          "User '"
              + getAuthenticatedInfo().getEmail()
              + "' does not have required action: "
              + IamAction.READ_DATA
              + " on snapshot ids"
              + inaccessibleIds);
    }

    var idsToQuery = requestIds.isEmpty() ? accessibleIds : requestIds;
    SearchQueryResultModel searchQueryResultModel =
        searchService.querySnapshot(searchQueryRequest, idsToQuery, offset, limit);
    return ResponseEntity.ok(searchQueryResultModel);
  }
}
