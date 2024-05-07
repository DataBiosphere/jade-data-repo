package bio.terra.app.controller;

import static bio.terra.app.utils.ControllerUtils.jobToResponse;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.app.utils.ControllerUtils;
import bio.terra.common.SqlSortDirection;
import bio.terra.common.ValidationUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.SnapshotsApi;
import bio.terra.model.AddAuthDomainResponseModel;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.FileModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.model.QueryDataRequestModel;
import bio.terra.model.ResourceLocks;
import bio.terra.model.SnapshotBuilderConceptsResponse;
import bio.terra.model.SnapshotBuilderCountRequest;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderGetConceptHierarchyResponse;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.model.SnapshotIdsAndRolesModel;
import bio.terra.model.SnapshotLinkDuosDatasetResponse;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPatchRequestModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.SqlSortDirectionAscDefault;
import bio.terra.model.TagCountResultModel;
import bio.terra.model.TagUpdateRequestModel;
import bio.terra.model.UnlockResourceRequest;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotRequestValidator;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import io.swagger.annotations.Api;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@Api(tags = {"snapshots"})
public class SnapshotsApiController implements SnapshotsApi {

  private final Logger logger = LoggerFactory.getLogger(SnapshotsApiController.class);

  // We do not include Access_Information since it can get expensive
  public static final String RETRIEVE_INCLUDE_DEFAULT_VALUE =
      "SOURCES,TABLES,RELATIONSHIPS,PROFILE,DATA_PROJECT,DUOS";

  private final HttpServletRequest request;
  private final JobService jobService;
  private final SnapshotRequestValidator snapshotRequestValidator;
  private final SnapshotService snapshotService;
  private final IamService iamService;
  private final IngestRequestValidator ingestRequestValidator;
  private final FileService fileService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final AssetModelValidator assetModelValidator;
  private final SnapshotBuilderService snapshotBuilderService;

  public SnapshotsApiController(
      HttpServletRequest request,
      JobService jobService,
      SnapshotRequestValidator snapshotRequestValidator,
      SnapshotService snapshotService,
      IamService iamService,
      IngestRequestValidator ingestRequestValidator,
      FileService fileService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      AssetModelValidator assetModelValidator,
      SnapshotBuilderService snapshotBuilderService) {
    this.request = request;
    this.jobService = jobService;
    this.snapshotRequestValidator = snapshotRequestValidator;
    this.snapshotService = snapshotService;
    this.iamService = iamService;
    this.ingestRequestValidator = ingestRequestValidator;
    this.fileService = fileService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.assetModelValidator = assetModelValidator;
    this.snapshotBuilderService = snapshotBuilderService;
  }

  @InitBinder
  protected void initBinder(final WebDataBinder binder) {
    binder.addValidators(snapshotRequestValidator);
    binder.addValidators(ingestRequestValidator);
    binder.addValidators(assetModelValidator);
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }

  // -- snapshot --
  private List<UUID> getUnauthorizedSources(
      List<UUID> snapshotSourceDatasetIds, AuthenticatedUserRequest userReq) {
    return snapshotSourceDatasetIds.stream()
        .filter(
            sourceId ->
                !iamService.isAuthorized(
                    userReq, IamResourceType.DATASET, sourceId.toString(), IamAction.LINK_SNAPSHOT))
        .collect(Collectors.toList());
  }

  @Override
  public ResponseEntity<JobModel> createSnapshot(
      @Valid @RequestBody SnapshotRequestModel snapshotRequestModel) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    List<UUID> snapshotSourceDatasetIds =
        snapshotService.getSourceDatasetIdsFromSnapshotRequest(snapshotRequestModel);
    // TODO: auth should be put into flight
    List<UUID> unauthorized = getUnauthorizedSources(snapshotSourceDatasetIds, userReq);
    if (unauthorized.isEmpty()) {
      String jobId = snapshotService.createSnapshot(snapshotRequestModel, userReq);
      // we can retrieve the job we just created
      return jobToResponse(jobService.retrieveJob(jobId, userReq));
    }
    throw new IamForbiddenException(
        "User is not authorized to create snapshots for these datasets " + unauthorized);
  }

  @Override
  public ResponseEntity<JobModel> deleteSnapshot(@PathVariable("id") UUID id) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifySnapshotAuthorization(userReq, id.toString(), IamAction.DELETE);
    String jobId = snapshotService.deleteSnapshot(id, userReq);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<SnapshotSummaryModel> patchSnapshot(
      UUID id, SnapshotPatchRequestModel patchRequest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    Set<IamAction> actions = snapshotService.patchSnapshotIamActions(patchRequest);
    verifySnapshotAuthorizations(userReq, id.toString(), actions);
    return ResponseEntity.ok(snapshotService.patch(id, patchRequest, userReq));
  }

  @Override
  public ResponseEntity<JobModel> exportSnapshot(
      @PathVariable("id") UUID id,
      Boolean exportGsPaths,
      Boolean validatePrimaryKeyUniqueness,
      Boolean signUrls) {
    logger.debug("Verifying user access");
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifySnapshotAuthorization(userReq, id.toString(), IamAction.EXPORT_SNAPSHOT);
    String jobId =
        snapshotService.exportSnapshot(
            id, userReq, exportGsPaths, validatePrimaryKeyUniqueness, signUrls);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<EnumerateSnapshotModel> enumerateSnapshots(
      Integer offset,
      Integer limit,
      EnumerateSortByParam sort,
      SqlSortDirectionAscDefault direction,
      String filter,
      String region,
      List<String> datasetIds,
      List<String> tags,
      List<String> duosIds) {
    ControllerUtils.validateEnumerateParams(offset, limit);
    List<UUID> datasetUUIDs =
        ListUtils.emptyIfNull(datasetIds).stream()
            .map(UUID::fromString)
            .collect(Collectors.toList());
    var esm =
        snapshotService.enumerateSnapshots(
            getAuthenticatedInfo(),
            offset,
            limit,
            sort,
            SqlSortDirection.from(direction),
            filter,
            region,
            datasetUUIDs,
            tags,
            duosIds);
    return ResponseEntity.ok(esm);
  }

  @Override
  public ResponseEntity<SnapshotIdsAndRolesModel> getSnapshotIdsAndRoles() {
    return ResponseEntity.ok(snapshotService.getSnapshotIdsAndRoles(getAuthenticatedInfo()));
  }

  @Override
  public ResponseEntity<SnapshotModel> retrieveSnapshot(
      UUID id,
      @RequestParam(
              value = "include",
              required = false,
              defaultValue = RETRIEVE_INCLUDE_DEFAULT_VALUE)
          List<SnapshotRetrieveIncludeModel> include) {
    logger.debug("Verifying user access");
    AuthenticatedUserRequest authenticatedInfo = getAuthenticatedInfo();
    snapshotService.verifySnapshotReadable(id, authenticatedInfo);
    logger.debug("Retrieving snapshot");
    SnapshotModel snapshotModel =
        snapshotService.retrieveSnapshotModel(id, include, authenticatedInfo);
    return ResponseEntity.ok(snapshotModel);
  }

  @Override
  public ResponseEntity<SnapshotSummaryModel> retrieveSnapshotSummary(UUID id) {
    AuthenticatedUserRequest authenticatedInfo = getAuthenticatedInfo();
    snapshotService.verifySnapshotListable(id, authenticatedInfo);
    SnapshotSummaryModel snapshotSummaryModel = snapshotService.retrieveSnapshotSummary(id);
    return ResponseEntity.ok(snapshotSummaryModel);
  }

  @Override
  public ResponseEntity<ResourceLocks> lockSnapshot(UUID id) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest, IamResourceType.DATASNAPSHOT, id.toString(), IamAction.LOCK_RESOURCE);
    return ResponseEntity.ok(snapshotService.manualExclusiveLock(userRequest, id));
  }

  @Override
  public ResponseEntity<ResourceLocks> unlockSnapshot(
      UUID id, UnlockResourceRequest unlockRequest) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest, IamResourceType.DATASNAPSHOT, id.toString(), IamAction.UNLOCK_RESOURCE);
    return ResponseEntity.ok(snapshotService.manualExclusiveUnlock(userRequest, id, unlockRequest));
  }

  @Override
  public ResponseEntity<List<FileModel>> listFiles(UUID id, Integer offset, Integer limit) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    verifySnapshotAuthorization(userRequest, id.toString(), IamAction.READ_DATA);
    List<FileModel> results = fileService.listSnapshotFiles(id.toString(), offset, limit);
    return ResponseEntity.ok(results);
  }

  @Override
  public ResponseEntity<FileModel> lookupSnapshotFileById(
      @PathVariable("id") UUID id,
      @PathVariable("fileid") String fileid,
      @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {

    verifySnapshotAuthorization(getAuthenticatedInfo(), id.toString(), IamAction.READ_DATA);
    FileModel fileModel = fileService.lookupSnapshotFile(id.toString(), fileid, depth);
    return ResponseEntity.ok(fileModel);
  }

  @Override
  public ResponseEntity<FileModel> lookupSnapshotFileByPath(
      @PathVariable("id") UUID id,
      @RequestParam(value = "path", required = true) String path,
      @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {

    verifySnapshotAuthorization(getAuthenticatedInfo(), id.toString(), IamAction.READ_DATA);
    if (!ValidationUtils.isValidPath(path)) {
      throw new ValidationException("InvalidPath");
    }
    FileModel fileModel = fileService.lookupSnapshotPath(id.toString(), path, depth);
    return ResponseEntity.ok(fileModel);
  }

  @Override
  public ResponseEntity<SnapshotPreviewModel> querySnapshotDataById(
      UUID id, String table, QueryDataRequestModel queryDataRequest) {
    snapshotService.verifySnapshotReadable(id, getAuthenticatedInfo());
    SqlSortDirection sortDirection = SqlSortDirection.from(queryDataRequest.getDirection());
    SnapshotPreviewModel previewModel =
        snapshotService.retrievePreview(
            getAuthenticatedInfo(),
            id,
            table,
            queryDataRequest.getLimit(),
            queryDataRequest.getOffset(),
            queryDataRequest.getSort(),
            sortDirection,
            queryDataRequest.getFilter());
    return ResponseEntity.ok(previewModel);
  }

  @Override
  public ResponseEntity<SnapshotPreviewModel> lookupSnapshotPreviewById(
      UUID id,
      String table,
      Integer offset,
      Integer limit,
      String sort,
      SqlSortDirectionAscDefault direction,
      String filter) {
    return querySnapshotDataById(
        id,
        table,
        new QueryDataRequestModel()
            .offset(offset)
            .limit(limit)
            .sort(sort)
            .direction(direction)
            .filter(filter));
  }

  // --snapshot data access controls (auth domains, group access constraints) --

  @Override
  public ResponseEntity<AddAuthDomainResponseModel> addSnapshotAuthDomain(
      UUID id, List<String> userGroups) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifySnapshotAuthorization(userReq, id.toString(), IamAction.UPDATE_AUTH_DOMAIN);
    AddAuthDomainResponseModel result =
        snapshotService.addSnapshotDataAccessControls(userReq, id, userGroups);
    return ResponseEntity.ok(result);
  }

  // --snapshot policies --
  @Override
  public ResponseEntity<PolicyResponse> addSnapshotPolicyMember(
      @PathVariable("id") UUID id,
      @PathVariable("policyName") String policyName,
      @Valid @RequestBody PolicyMemberRequest policyMember) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    PolicyModel policy =
        iamService.addPolicyMember(
            userReq, IamResourceType.DATASNAPSHOT, id, policyName, policyMember.getEmail());
    PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
    return ResponseEntity.ok(response);
  }

  @Override
  public ResponseEntity<PolicyResponse> retrieveSnapshotPolicies(UUID id) {
    PolicyResponse response = snapshotService.retrieveSnapshotPolicies(id, getAuthenticatedInfo());
    return ResponseEntity.ok(response);
  }

  @Override
  public ResponseEntity<PolicyResponse> deleteSnapshotPolicyMember(
      @PathVariable("id") UUID id,
      @PathVariable("policyName") String policyName,
      @PathVariable("memberEmail") String memberEmail) {
    // member email can't be null since it is part of the URL
    if (!ValidationUtils.isValidEmail(memberEmail)) {
      throw new ValidationException("InvalidMemberEmail");
    }
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    PolicyModel policy =
        iamService.deletePolicyMember(
            userReq, IamResourceType.DATASNAPSHOT, id, policyName, memberEmail);
    PolicyResponse response = new PolicyResponse().policies(Collections.singletonList(policy));
    return ResponseEntity.ok(response);
  }

  @Override
  public ResponseEntity<List<String>> retrieveUserSnapshotRoles(UUID id) {
    List<String> roles = snapshotService.retrieveUserSnapshotRoles(id, getAuthenticatedInfo());
    return ResponseEntity.ok(roles);
  }

  @Override
  public ResponseEntity<SnapshotLinkDuosDatasetResponse> linkDuosDatasetToSnapshot(
      UUID id, String duosId) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifySnapshotAuthorization(userReq, id.toString(), IamAction.SHARE_POLICY_READER);
    return ResponseEntity.ok(snapshotService.updateSnapshotDuosDataset(id, userReq, duosId));
  }

  @Override
  public ResponseEntity<SnapshotLinkDuosDatasetResponse> unlinkDuosDatasetFromSnapshot(UUID id) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifySnapshotAuthorization(userReq, id.toString(), IamAction.SHARE_POLICY_READER);
    return ResponseEntity.ok(snapshotService.updateSnapshotDuosDataset(id, userReq, null));
  }

  @Override
  public ResponseEntity<TagCountResultModel> getSnapshotTags(String filter, Integer limit) {
    return ResponseEntity.ok(snapshotService.getTags(getAuthenticatedInfo(), filter, limit));
  }

  @Override
  public ResponseEntity<SnapshotSummaryModel> updateSnapshotTags(
      UUID id, TagUpdateRequestModel tagUpdateRequest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    verifySnapshotAuthorization(userReq, id.toString(), IamAction.UPDATE_SNAPSHOT);
    return ResponseEntity.ok(snapshotService.updateTags(id, tagUpdateRequest));
  }

  @Override
  public ResponseEntity<SnapshotBuilderConceptsResponse> getConceptChildren(
      UUID id, Integer conceptId) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    verifySnapshotAuthorization(userRequest, id.toString(), IamAction.READ_AGGREGATE_DATA);
    return ResponseEntity.ok(snapshotBuilderService.getConceptChildren(id, conceptId, userRequest));
  }

  @Override
  public ResponseEntity<SnapshotBuilderConceptsResponse> enumerateConcepts(
      UUID id, Integer domainId, String filterText) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    verifySnapshotAuthorization(userRequest, id.toString(), IamAction.READ_AGGREGATE_DATA);
    return ResponseEntity.ok(
        snapshotBuilderService.enumerateConcepts(id, domainId, filterText, userRequest));
  }

  @Override
  public ResponseEntity<SnapshotBuilderGetConceptHierarchyResponse> getConceptHierarchy(
      UUID id, Integer conceptId) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    verifySnapshotAuthorization(userRequest, id.toString(), IamAction.READ_AGGREGATE_DATA);
    return ResponseEntity.ok(
        snapshotBuilderService.getConceptHierarchy(id, conceptId, userRequest));
  }

  @Override
  public ResponseEntity<SnapshotBuilderCountResponse> getSnapshotBuilderCount(
      UUID id, SnapshotBuilderCountRequest body) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    verifySnapshotAuthorization(userRequest, id.toString(), IamAction.READ_AGGREGATE_DATA);
    return ResponseEntity.ok(
        snapshotBuilderService.getCountResponse(id, body.getCohorts(), userRequest));
  }

  private void verifySnapshotAuthorization(
      AuthenticatedUserRequest userReq, String resourceId, IamAction action) {
    IamResourceType resourceType = IamResourceType.DATASNAPSHOT;
    // Check if snapshot exists
    snapshotService.retrieveSnapshotSummary(UUID.fromString(resourceId));
    // Verify snapshot permissions
    iamService.verifyAuthorization(userReq, resourceType, resourceId, action);
  }

  private void verifySnapshotAuthorizations(
      AuthenticatedUserRequest userReq, String resourceId, Collection<IamAction> actions) {
    // Check if snapshot exists
    snapshotService.retrieveSnapshotSummary(UUID.fromString(resourceId));
    // Verify snapshot permissions
    iamService.verifyAuthorizations(userReq, IamResourceType.DATASNAPSHOT, resourceId, actions);
  }

  @Override
  public ResponseEntity<SnapshotBuilderSettings> getSnapshotSnapshotBuilderSettings(UUID id) {
    iamService.verifyAuthorization(
        getAuthenticatedInfo(),
        IamResourceType.DATASNAPSHOT,
        id.toString(),
        IamAction.GET_SNAPSHOT_BUILDER_SETTINGS);
    return ResponseEntity.ok(snapshotService.getSnapshotBuilderSettings(id));
  }

  @Override
  public ResponseEntity<SnapshotBuilderSettings> updateSnapshotSnapshotBuilderSettings(
      UUID id, SnapshotBuilderSettings settings) {
    iamService.verifyAuthorization(
        getAuthenticatedInfo(),
        IamResourceType.DATASNAPSHOT,
        id.toString(),
        IamAction.UPDATE_SNAPSHOT);
    snapshotService.updateSnapshotBuilderSettings(id, settings);
    return ResponseEntity.ok(settings);
  }

  @Override
  public ResponseEntity<Void> deleteSnapshotSnapshotBuilderSettings(UUID id) {
    iamService.verifyAuthorization(
        getAuthenticatedInfo(),
        IamResourceType.DATASNAPSHOT,
        id.toString(),
        IamAction.UPDATE_SNAPSHOT);
    snapshotService.deleteSnapshotBuilderSettings(id);
    return ResponseEntity.ok().build();
  }
}
