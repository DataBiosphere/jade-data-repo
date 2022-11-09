package bio.terra.app.controller;

import static bio.terra.app.utils.ControllerUtils.jobToResponse;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.app.utils.ControllerUtils;
import bio.terra.common.ValidationUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.SnapshotsApi;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.FileModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.model.SnapshotLinkDuosDatasetResponse;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPatchRequestModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.SqlSortDirection;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final JobService jobService;
  private final SnapshotRequestValidator snapshotRequestValidator;
  private final SnapshotService snapshotService;
  private final IamService iamService;
  private final IngestRequestValidator ingestRequestValidator;
  private final FileService fileService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final AssetModelValidator assetModelValidator;

  @Autowired
  public SnapshotsApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      JobService jobService,
      SnapshotRequestValidator snapshotRequestValidator,
      SnapshotService snapshotService,
      IamService iamService,
      IngestRequestValidator ingestRequestValidator,
      FileService fileService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      AssetModelValidator assetModelValidator) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.jobService = jobService;
    this.snapshotRequestValidator = snapshotRequestValidator;
    this.snapshotService = snapshotService;
    this.iamService = iamService;
    this.ingestRequestValidator = ingestRequestValidator;
    this.fileService = fileService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.assetModelValidator = assetModelValidator;
  }

  @InitBinder
  protected void initBinder(final WebDataBinder binder) {
    binder.addValidators(snapshotRequestValidator);
    binder.addValidators(ingestRequestValidator);
    binder.addValidators(assetModelValidator);
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
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASNAPSHOT, id.toString(), IamAction.DELETE);
    String jobId = snapshotService.deleteSnapshot(id, userReq);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<SnapshotSummaryModel> patchSnapshot(
      UUID id, SnapshotPatchRequestModel patchRequest) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    Set<IamAction> actions = snapshotService.patchSnapshotIamActions(patchRequest);
    iamService.verifyAuthorizations(userReq, IamResourceType.DATASNAPSHOT, id.toString(), actions);
    return ResponseEntity.ok(snapshotService.patch(id, patchRequest, userReq));
  }

  @Override
  public ResponseEntity<JobModel> exportSnapshot(
      @PathVariable("id") UUID id, Boolean exportGsPaths, Boolean validatePrimaryKeyUniqueness) {
    logger.info("Verifying user access");
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASNAPSHOT, id.toString(), IamAction.EXPORT_SNAPSHOT);
    String jobId =
        snapshotService.exportSnapshot(id, userReq, exportGsPaths, validatePrimaryKeyUniqueness);
    // we can retrieve the job we just created
    return jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<EnumerateSnapshotModel> enumerateSnapshots(
      Integer offset,
      Integer limit,
      EnumerateSortByParam sort,
      SqlSortDirection direction,
      String filter,
      String region,
      List<String> datasetIds) {
    ControllerUtils.validateEnumerateParams(offset, limit);
    List<UUID> datasetUUIDs =
        ListUtils.emptyIfNull(datasetIds).stream()
            .map(UUID::fromString)
            .collect(Collectors.toList());
    var esm =
        snapshotService.enumerateSnapshots(
            getAuthenticatedInfo(), offset, limit, sort, direction, filter, region, datasetUUIDs);
    return ResponseEntity.ok(esm);
  }

  @Override
  public ResponseEntity<SnapshotModel> retrieveSnapshot(
      UUID id, List<SnapshotRetrieveIncludeModel> include) {
    logger.info("Verifying user access");
    AuthenticatedUserRequest authenticatedInfo = getAuthenticatedInfo();
    snapshotService.verifySnapshotAccessible(id, authenticatedInfo);
    logger.info("Retrieving snapshot");
    SnapshotModel snapshotModel =
        snapshotService.retrieveAvailableSnapshotModel(id, include, authenticatedInfo);
    return ResponseEntity.ok(snapshotModel);
  }

  @Override
  public ResponseEntity<FileModel> lookupSnapshotFileById(
      @PathVariable("id") UUID id,
      @PathVariable("fileid") String fileid,
      @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {

    iamService.verifyAuthorization(
        getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT, id.toString(), IamAction.READ_DATA);
    FileModel fileModel = fileService.lookupSnapshotFile(id.toString(), fileid, depth);
    return ResponseEntity.ok(fileModel);
  }

  @Override
  public ResponseEntity<FileModel> lookupSnapshotFileByPath(
      @PathVariable("id") UUID id,
      @RequestParam(value = "path", required = true) String path,
      @RequestParam(value = "depth", required = false, defaultValue = "0") Integer depth) {

    iamService.verifyAuthorization(
        getAuthenticatedInfo(), IamResourceType.DATASNAPSHOT, id.toString(), IamAction.READ_DATA);
    if (!ValidationUtils.isValidPath(path)) {
      throw new ValidationException("InvalidPath");
    }
    FileModel fileModel = fileService.lookupSnapshotPath(id.toString(), path, depth);
    return ResponseEntity.ok(fileModel);
  }

  @Override
  public ResponseEntity<SnapshotPreviewModel> lookupSnapshotPreviewById(
      UUID id,
      String table,
      Integer offset,
      Integer limit,
      String sort,
      SqlSortDirection direction,
      String filter) {
    logger.info("Verifying user access");
    snapshotService.verifySnapshotAccessible(id, getAuthenticatedInfo());
    logger.info("Retrieving snapshot id {}", id);
    // TODO: Remove after https://broadworkbench.atlassian.net/browse/DR-2588 is fixed
    SqlSortDirection sortDirection = Objects.requireNonNullElse(direction, SqlSortDirection.ASC);
    SnapshotPreviewModel previewModel =
        snapshotService.retrievePreview(
            getAuthenticatedInfo(), id, table, limit, offset, sort, sortDirection, filter);
    return ResponseEntity.ok(previewModel);
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
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASNAPSHOT, id.toString(), IamAction.SHARE_POLICY_READER);
    return ResponseEntity.ok(snapshotService.updateSnapshotDuosDataset(id, userReq, duosId));
  }

  @Override
  public ResponseEntity<SnapshotLinkDuosDatasetResponse> unlinkDuosDatasetFromSnapshot(UUID id) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATASNAPSHOT, id.toString(), IamAction.SHARE_POLICY_READER);
    return ResponseEntity.ok(snapshotService.updateSnapshotDuosDataset(id, userReq, null));
  }
}
