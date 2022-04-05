package bio.terra.app.controller;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.controller.exception.ApiException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.SearchApi;
import bio.terra.model.SearchIndexModel;
import bio.terra.model.SearchIndexRequest;
import bio.terra.model.SearchMetadataModel;
import bio.terra.model.SearchMetadataResponse;
import bio.terra.model.SearchQueryRequest;
import bio.terra.model.SearchQueryResultModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.search.SearchService;
import bio.terra.service.search.SnapshotSearchMetadataDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.swagger.annotations.Api;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  private static final Logger logger = LoggerFactory.getLogger(SearchApiController.class);

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
  public ResponseEntity<SearchMetadataResponse> enumerateSnapshotSearch() {
    Map<UUID, Set<IamRole>> idsAndRoles =
        iamService.listAuthorizedResources(getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT);
    Map<UUID, String> metadata = snapshotSearchMetadataDao.getMetadata(idsAndRoles.keySet());
    var response = new SearchMetadataResponse();
    metadata.forEach(
        (uuid, data) -> {
          JsonNode node = toJsonNode(data);
          ArrayNode roles = objectMapper.createArrayNode();
          for (IamRole iamRole : idsAndRoles.get(uuid)) {
            roles.add(TextNode.valueOf(iamRole.toString()));
          }
          ((ObjectNode) node).set("roles", roles);
          response.addResultItem(node);
        });
    return ResponseEntity.ok(response);
  }

  private JsonNode toJsonNode(String json) {
    try {
      return objectMapper.readValue(json, JsonNode.class);
    } catch (JsonProcessingException e) {
      // This shouldn't occur, as the data stored in postgres must be valid JSON, because it's
      // stored as JSONB.
      throw new RuntimeException(e);
    }
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

  // TODO: add unit test in SearchAPIControllerTest
  @Override
  public ResponseEntity<SearchQueryResultModel> querySearchIndices(
      SearchQueryRequest searchQueryRequest, Integer offset, Integer limit) {

    Set<UUID> accessibleIds =
        iamService
            .listAuthorizedResources(getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT)
            .keySet();

    final List<UUID> snapshotIds = searchQueryRequest.getSnapshotIds();

    Set<UUID> requestIds =
        snapshotIds == null || snapshotIds.isEmpty() ? Set.of() : Set.copyOf(snapshotIds);

    Set<UUID> inaccessibleIds = new HashSet<>(requestIds);
    inaccessibleIds.removeAll(accessibleIds);
    if (!inaccessibleIds.isEmpty()) {
      throw new IamForbiddenException(
          "User '"
              + getAuthenticatedInfo().getEmail()
              + "' does not have required action: "
              + IamAction.READ_DATA
              + " on snapshot ids "
              + inaccessibleIds);
    }

    var idsToQuery = requestIds.isEmpty() ? accessibleIds : requestIds;
    SearchQueryResultModel searchQueryResultModel =
        searchService.querySnapshot(searchQueryRequest, idsToQuery, offset, limit);
    return ResponseEntity.ok(searchQueryResultModel);
  }
}
