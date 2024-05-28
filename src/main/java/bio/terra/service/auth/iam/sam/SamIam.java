package bio.terra.service.auth.iam.sam;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.ExceptionUtils;
import bio.terra.common.ValidationUtils;
import bio.terra.common.exception.ErrorReportException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetRequestModelPolicies;
import bio.terra.model.PolicyModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.model.ResourcePolicyModel;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.SnapshotRequestModelPolicies;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.exception.IamBadRequestException;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.auth.iam.exception.IamInternalServerErrorException;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.auth.iam.exception.IamUnauthorizedException;
import bio.terra.service.configuration.ConfigurationService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpStatusCodes;
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembershipRequest;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembershipV2;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntryV2;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.broadinstitute.dsde.workbench.client.sam.model.ErrorReport;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;
import org.broadinstitute.dsde.workbench.client.sam.model.ManagedResourceGroupCoordinates;
import org.broadinstitute.dsde.workbench.client.sam.model.RequesterPaysSignedUrlRequest;
import org.broadinstitute.dsde.workbench.client.sam.model.RolesAndActions;
import org.broadinstitute.dsde.workbench.client.sam.model.SyncReportEntry;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("iamProvider")
// Use @Profile to select when there is more than one IamService
public class SamIam implements IamProviderInterface {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final SamConfiguration samConfig;
  private final ConfigurationService configurationService;
  private final SamApiService samApiService;

  @Autowired
  public SamIam(
      SamConfiguration samConfig,
      ConfigurationService configurationService,
      SamApiService samApiService) {
    this.samConfig = samConfig;
    this.configurationService = configurationService;
    this.samApiService = samApiService;
  }

  private static final Logger logger = LoggerFactory.getLogger(SamIam.class);

  /**
   * Asks SAM if a user can do an action on a resource. This method converts the SAM-specific
   * ApiException to a data repo-specific common exception.
   *
   * @return true if authorized, false otherwise
   */
  @Override
  public boolean isAuthorized(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      String resourceId,
      IamAction action)
      throws InterruptedException {

    return SamRetry.retry(
        configurationService,
        () -> isAuthorizedInner(userReq, iamResourceType, resourceId, action));
  }

  private boolean isAuthorizedInner(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      String resourceId,
      IamAction action)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    boolean authorized =
        samResourceApi.resourcePermissionV2(
            iamResourceType.toString(), resourceId, action.toString());
    logger.debug("authorized is " + authorized);
    return authorized;
  }

  @WithSpan
  @Override
  public Map<UUID, Set<IamRole>> listAuthorizedResources(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType)
      throws InterruptedException {
    return SamRetry.retry(
        configurationService, () -> listAuthorizedResourcesInner(userReq, iamResourceType));
  }

  private Map<UUID, Set<IamRole>> listAuthorizedResourcesInner(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType) throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    return samResourceApi.listResourcesAndPoliciesV2(iamResourceType.getSamResourceName()).stream()
        .filter(resource -> ValidationUtils.isValidUuid(resource.getResourceId()))
        .collect(
            Collectors.groupingBy(
                resource -> UUID.fromString(resource.getResourceId()),
                Collectors.flatMapping(
                    r ->
                        Objects.requireNonNullElse(r.getDirect(), new RolesAndActions())
                            .getRoles()
                            .stream()
                            .map(IamRole::fromValue),
                    Collectors.toSet())));
  }

  @WithSpan
  @Override
  public List<String> listActions(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId)
      throws InterruptedException {
    return SamRetry.retry(
        configurationService, () -> listActionsInner(userReq, iamResourceType, resourceId));
  }

  private List<String> listActionsInner(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    return samResourceApi.resourceActionsV2(iamResourceType.toString(), resourceId);
  }

  @WithSpan
  @Override
  public boolean hasAnyActions(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId)
      throws InterruptedException {
    return listActions(userReq, iamResourceType, resourceId).size() > 0;
  }

  @Override
  public void deleteDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId)
      throws InterruptedException {
    deleteResource(userReq, IamResourceType.DATASET, datasetId.toString());
  }

  @Override
  public void deleteSnapshotResource(AuthenticatedUserRequest userReq, UUID snapshotId)
      throws InterruptedException {
    deleteResource(userReq, IamResourceType.DATASNAPSHOT, snapshotId.toString());
  }

  private void deleteResource(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId)
      throws InterruptedException {
    SamRetry.retry(
        configurationService, () -> deleteResourceInner(userReq, iamResourceType, resourceId));
  }

  private void deleteResourceInner(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    samResourceApi.deleteResourceV2(iamResourceType.toString(), resourceId);
  }

  @Override
  public Map<IamRole, String> createDatasetResource(
      AuthenticatedUserRequest userReq, UUID datasetId, DatasetRequestModelPolicies policies)
      throws InterruptedException {
    SamRetry.retry(
        configurationService, () -> createDatasetResourceInnerV2(userReq, datasetId, policies));
    return SamRetry.retry(
        configurationService, () -> syncDatasetResourcePoliciesInner(userReq, datasetId));
  }

  private void createDatasetResourceInnerV2(
      AuthenticatedUserRequest userReq, UUID datasetId, DatasetRequestModelPolicies policies)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    CreateResourceRequestV2 req = createDatasetResourceRequest(userReq, datasetId, policies);
    samResourceApi.createResourceV2(IamResourceType.DATASET.toString(), req);
  }

  @VisibleForTesting
  CreateResourceRequestV2 createDatasetResourceRequest(
      AuthenticatedUserRequest userReq, UUID datasetId, DatasetRequestModelPolicies policies) {
    policies = Optional.ofNullable(policies).orElse(new DatasetRequestModelPolicies());
    UserStatusInfo userStatusInfo = getUserInfoAndVerify(userReq);

    CreateResourceRequestV2 req = new CreateResourceRequestV2().resourceId(datasetId.toString());

    req.putPoliciesItem(
        IamRole.ADMIN.toString(), createAccessPolicy(IamRole.ADMIN, getAdminEmailList()));

    List<String> stewards = new ArrayList<>();
    stewards.add(userStatusInfo.getUserEmail());
    stewards.addAll(ListUtils.emptyIfNull(policies.getStewards()));
    req.putPoliciesItem(IamRole.STEWARD.toString(), createAccessPolicy(IamRole.STEWARD, stewards));

    req.putPoliciesItem(
        IamRole.CUSTODIAN.toString(),
        createAccessPolicy(IamRole.CUSTODIAN, policies.getCustodians()));

    req.putPoliciesItem(
        IamRole.SNAPSHOT_CREATOR.toString(),
        createAccessPolicy(IamRole.SNAPSHOT_CREATOR, policies.getSnapshotCreators()));

    req.authDomain(List.of());
    logger.debug("SAM request: " + req);
    return req;
  }

  private Map<IamRole, String> syncDatasetResourcePoliciesInner(
      AuthenticatedUserRequest userReq, UUID datasetId) throws ApiException {
    // This includes multiple calls to SAM within one retry call
    // retrySyncDatasetPolicies() integration test proves that we can re-run all of these calls
    // if the call fails part of the way through

    // we'll want all of these roles to have read access to the underlying data,
    // so we sync and return the emails for the policies that get created by SAM
    // Note: ADMIN explicitly does NOT require this since it does not require read access to the
    // data
    Map<IamRole, String> policies = new HashMap<>();
    for (IamRole role :
        Arrays.asList(IamRole.STEWARD, IamRole.CUSTODIAN, IamRole.SNAPSHOT_CREATOR)) {
      String policy = syncOnePolicy(userReq, IamResourceType.DATASET, datasetId, role);
      policies.put(role, policy);
    }

    return policies;
  }

  @Override
  public Map<IamRole, String> createSnapshotResource(
      AuthenticatedUserRequest userReq, UUID snapshotId, SnapshotRequestModelPolicies policies)
      throws InterruptedException {
    SamRetry.retry(
        configurationService, () -> createSnapshotResourceInnerV2(userReq, snapshotId, policies));
    return SamRetry.retry(
        configurationService, () -> syncSnapshotResourcePoliciesInner(userReq, snapshotId));
  }

  private void createSnapshotResourceInnerV2(
      AuthenticatedUserRequest userReq, UUID snapshotId, SnapshotRequestModelPolicies policies)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    CreateResourceRequestV2 req = createSnapshotResourceRequest(userReq, snapshotId, policies);
    samResourceApi.createResourceV2(IamResourceType.DATASNAPSHOT.toString(), req);
  }

  @VisibleForTesting
  CreateResourceRequestV2 createSnapshotResourceRequest(
      AuthenticatedUserRequest userReq, UUID snapshotId, SnapshotRequestModelPolicies policies) {
    policies = Optional.ofNullable(policies).orElse(new SnapshotRequestModelPolicies());
    UserStatusInfo userStatusInfo = getUserInfoAndVerify(userReq);
    CreateResourceRequestV2 req = new CreateResourceRequestV2().resourceId(snapshotId.toString());

    req.putPoliciesItem(
        IamRole.ADMIN.toString(), createAccessPolicy(IamRole.ADMIN, getAdminEmailList()));

    List<String> stewards = new ArrayList<>();
    stewards.add(userStatusInfo.getUserEmail());
    stewards.addAll(ListUtils.emptyIfNull(policies.getStewards()));
    req.putPoliciesItem(IamRole.STEWARD.toString(), createAccessPolicy(IamRole.STEWARD, stewards));

    req.putPoliciesItem(
        IamRole.READER.toString(), createAccessPolicy(IamRole.READER, policies.getReaders()));

    req.putPoliciesItem(
        IamRole.DISCOVERER.toString(),
        createAccessPolicy(IamRole.DISCOVERER, policies.getDiscoverers()));

    req.putPoliciesItem(
        IamRole.AGGREGATE_DATA_READER.toString(),
        createAccessPolicy(IamRole.AGGREGATE_DATA_READER, policies.getAggregateDataReaders()));

    req.authDomain(List.of());
    logger.debug("SAM request: " + req);
    return req;
  }

  private Map<IamRole, String> syncSnapshotResourcePoliciesInner(
      AuthenticatedUserRequest userReq, UUID snapshotId) throws ApiException {
    Map<IamRole, String> policies = new HashMap<>();
    // sync the policies for all roles that have IamAction.READ_DATA
    for (IamRole role : Arrays.asList(IamRole.STEWARD, IamRole.READER)) {
      String policy = syncOnePolicy(userReq, IamResourceType.DATASNAPSHOT, snapshotId, role);
      policies.put(role, policy);
    }
    return policies;
  }

  @Override
  public Map<IamRole, List<String>> createSnapshotBuilderRequestResource(
      AuthenticatedUserRequest userReq, UUID snapshotId, UUID snapshotBuilderRequestId)
      throws InterruptedException {
    var initialRoles = Map.of(IamRole.OWNER, List.of(userReq.getEmail()));
    SamRetry.retry(
        configurationService,
        () ->
            createSnapshotBuilderRequestResourceInner(
                userReq, snapshotId, snapshotBuilderRequestId, initialRoles));
    return initialRoles;
  }

  private void createSnapshotBuilderRequestResourceInner(
      AuthenticatedUserRequest userReq,
      UUID snapshotId,
      UUID snapshotBuilderRequestId,
      Map<IamRole, List<String>> initialRoles)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    CreateResourceRequestV2 req =
        createSnapshotBuilderRequestResourceRequest(
            userReq, snapshotId, snapshotBuilderRequestId, initialRoles);
    samResourceApi.createResourceV2(IamResourceType.SNAPSHOT_BUILDER_REQUEST.toString(), req);
  }

  private CreateResourceRequestV2 createSnapshotBuilderRequestResourceRequest(
      AuthenticatedUserRequest userReq,
      UUID snapshotId,
      UUID snapshotBuilderRequestId,
      Map<IamRole, List<String>> initialRoles) {
    getUserInfoAndVerify(userReq);
    FullyQualifiedResourceId parentId =
        new FullyQualifiedResourceId()
            .resourceTypeName(IamResourceType.DATASNAPSHOT.toString())
            .resourceId(snapshotId.toString());
    CreateResourceRequestV2 req =
        new CreateResourceRequestV2()
            .resourceId(snapshotBuilderRequestId.toString())
            .parent(parentId)
            .authDomain(List.of())
            .policies(
                initialRoles.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            entry -> entry.getKey().toString(),
                            entry -> createAccessPolicy(entry.getKey(), entry.getValue()))));

    logger.debug(String.format("SAM request: %s", req));
    return req;
  }

  private String syncOnePolicy(
      AuthenticatedUserRequest userReq, IamResourceType resourceType, UUID id, IamRole role)
      throws ApiException {
    Map<String, List<SyncReportEntry>> results =
        samApiService
            .googleApi(userReq.getToken())
            .syncPolicy(resourceType.toString(), id.toString(), role.toString(), null);
    String policyEmail = getPolicyGroupEmailFromResponse(results);
    logger.debug(
        "Policy Group Resource: {} Role: {} Email:  {} ",
        resourceType.toString(),
        role.toString(),
        policyEmail);
    return policyEmail;
  }

  @Override
  public void createProfileResource(AuthenticatedUserRequest userReq, String profileId)
      throws InterruptedException {
    SamRetry.retry(configurationService, () -> createProfileResourceInnerV2(userReq, profileId));
  }

  private void createProfileResourceInnerV2(AuthenticatedUserRequest userReq, String profileId)
      throws ApiException {
    UserStatusInfo userStatusInfo = getUserInfoAndVerify(userReq);
    CreateResourceRequestV2 req = new CreateResourceRequestV2();
    req.setResourceId(profileId);
    req.putPoliciesItem(
        IamRole.ADMIN.toString(), createAccessPolicy(IamRole.ADMIN, getAdminEmailList()));
    req.putPoliciesItem(
        IamRole.OWNER.toString(),
        createAccessPolicyOne(IamRole.OWNER, userStatusInfo.getUserEmail()));
    req.putPoliciesItem(IamRole.USER.toString(), createAccessPolicy(IamRole.USER, null));
    req.authDomain(List.of());

    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    logger.debug("SAM request: " + req);

    samResourceApi.createResourceV2(IamResourceType.SPEND_PROFILE.toString(), req);
  }

  private List<String> getAdminEmailList() {
    return StringUtils.isBlank(samConfig.adminsGroupEmail())
        ? List.of()
        : List.of(samConfig.adminsGroupEmail());
  }

  @Override
  public void deleteProfileResource(AuthenticatedUserRequest userReq, String profileId)
      throws InterruptedException {
    deleteResource(userReq, IamResourceType.SPEND_PROFILE, profileId.toString());
  }

  @Override
  public List<String> retrieveAuthDomain(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws InterruptedException {
    return SamRetry.retry(
        configurationService, () -> retrieveAuthDomainInner(userReq, iamResourceType, resourceId));
  }

  public List<String> retrieveAuthDomainInner(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    return samResourceApi.getAuthDomainV2(
        iamResourceType.getSamResourceName(), resourceId.toString());
  }

  @Override
  public void patchAuthDomain(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      List<String> userGroups)
      throws InterruptedException {
    SamRetry.retry(
        configurationService,
        () -> patchAuthDomainInner(userReq, iamResourceType, resourceId, userGroups));
  }

  public void patchAuthDomainInner(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      List<String> userGroups)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    samResourceApi.patchAuthDomainV2(
        iamResourceType.getSamResourceName(), resourceId.toString(), userGroups);
  }

  @Override
  public List<SamPolicyModel> retrievePolicies(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws InterruptedException {
    return SamRetry.retry(
        configurationService, () -> retrievePoliciesInner(userReq, iamResourceType, resourceId));
  }

  @Override
  public List<String> retrieveUserRoles(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws InterruptedException {
    return SamRetry.retry(
        configurationService, () -> retrieveUserRolesInner(userReq, iamResourceType, resourceId));
  }

  private List<String> retrieveUserRolesInner(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    return samResourceApi.resourceRolesV2(
        iamResourceType.getSamResourceName(), resourceId.toString());
  }

  private List<SamPolicyModel> retrievePoliciesInner(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    try (Stream<AccessPolicyResponseEntryV2> resultStream =
        samResourceApi
            .listResourcePoliciesV2(iamResourceType.toString(), resourceId.toString())
            .stream()) {
      return resultStream
          .map(
              entry ->
                  new SamPolicyModel()
                      .name(entry.getPolicyName())
                      .members(entry.getPolicy().getMemberEmails())
                      .memberPolicies(
                          Optional.ofNullable(entry.getPolicy().getMemberPolicies())
                              .orElseGet(List::of)
                              .stream()
                              .map(
                                  pid ->
                                      new ResourcePolicyModel()
                                          .policyName(pid.getPolicyName())
                                          .policyEmail(pid.getPolicyEmail())
                                          .resourceId(UUID.fromString(pid.getResourceId()))
                                          .resourceTypeName(pid.getResourceTypeName()))
                              .collect(Collectors.toList())))
          .collect(Collectors.toList());
    }
  }

  @Override
  public Map<IamRole, String> retrievePolicyEmails(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws InterruptedException {
    return SamRetry.retry(
        configurationService,
        () -> retrievePolicyEmailsInner(userReq, iamResourceType, resourceId));
  }

  private Map<IamRole, String> retrievePolicyEmailsInner(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    try (Stream<AccessPolicyResponseEntryV2> resultStream =
        samResourceApi
            .listResourcePoliciesV2(iamResourceType.toString(), resourceId.toString())
            .stream()) {
      return resultStream.collect(
          Collectors.toMap(
              a -> IamRole.fromValue(a.getPolicyName()), AccessPolicyResponseEntryV2::getEmail));
    }
  }

  @Override
  public PolicyModel addPolicyMember(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      String policyName,
      String userEmail)
      throws InterruptedException {
    SamRetry.retry(
        configurationService,
        () -> addPolicyMemberInner(userReq, iamResourceType, resourceId, policyName, userEmail));
    return SamRetry.retry(
        configurationService,
        () -> retrievePolicy(userReq, iamResourceType, resourceId, policyName));
  }

  private void addPolicyMemberInner(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      String policyName,
      String userEmail)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    logger.debug(
        "addUserPolicy resourceType {} resourceId {} policyName {} userEmail {}",
        iamResourceType.toString(),
        resourceId.toString(),
        policyName,
        userEmail);
    samResourceApi.addUserToPolicyV2(
        iamResourceType.toString(), resourceId.toString(), policyName, userEmail, null);
  }

  @Override
  public PolicyModel deletePolicyMember(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      String policyName,
      String userEmail)
      throws InterruptedException {
    SamRetry.retry(
        configurationService,
        () -> deletePolicyMemberInner(userReq, iamResourceType, resourceId, policyName, userEmail));
    return SamRetry.retry(
        configurationService,
        () -> retrievePolicy(userReq, iamResourceType, resourceId, policyName));
  }

  private void deletePolicyMemberInner(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      String policyName,
      String userEmail)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    samResourceApi.removeUserFromPolicyV2(
        iamResourceType.toString(), resourceId.toString(), policyName, userEmail);
  }

  private PolicyModel retrievePolicy(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      String policyName)
      throws ApiException {
    ResourcesApi samResourceApi = samApiService.resourcesApi(userReq.getToken());
    AccessPolicyMembershipV2 result =
        samResourceApi.getPolicyV2(iamResourceType.toString(), resourceId.toString(), policyName);
    return new PolicyModel().name(policyName).members(result.getMemberEmails());
  }

  @Override
  public UserStatusInfo getUserInfo(AuthenticatedUserRequest userReq) {
    UsersApi samUsersApi = samApiService.usersApi(userReq.getToken());
    try {
      org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo samInfo =
          samUsersApi.getUserStatusInfo();
      return new UserStatusInfo()
          .userSubjectId(samInfo.getUserSubjectId())
          .userEmail(samInfo.getUserEmail())
          .enabled(samInfo.getEnabled());
    } catch (ApiException ex) {
      throw convertSAMExToDataRepoEx(ex);
    }
  }

  @Override
  public String getProxyGroup(AuthenticatedUserRequest userReq) throws InterruptedException {
    return SamRetry.retry(configurationService, () -> getProxyGroupInner(userReq));
  }

  private String getProxyGroupInner(AuthenticatedUserRequest userReq) throws ApiException {
    return samApiService.googleApi(userReq.getToken()).getProxyGroup(userReq.getEmail());
  }

  @Override
  public RepositoryStatusModelSystems samStatus() {
    try {
      return SamRetry.retry(
          configurationService,
          () -> {
            StatusApi samApi = samApiService.statusApi();
            SystemStatus status = samApi.getSystemStatus();
            return new RepositoryStatusModelSystems()
                .ok(status.getOk())
                .message(status.getSystems().toString());
          });
    } catch (Exception ex) {
      String errorMsg = "Sam status check failed";
      logger.error(errorMsg, ex);
      return new RepositoryStatusModelSystems()
          .ok(false)
          .message(errorMsg + ": " + ExceptionUtils.formatException(ex));
    }
  }

  @Override
  public void registerUser(String accessToken) throws InterruptedException {
    logger.info("Registering the ingest service account into Terra");
    SamRetry.retry(
        configurationService,
        () -> {
          try {
            logger.info("Running the registration process");
            return samApiService.usersApi(accessToken).createUserV2(null);
          } catch (ApiException e) {
            // This conflict could happen if the request timed out originally.
            // In that case, it's ok to assume that this is a success and move on
            if (e.getCode() == 409) {
              logger.warn("User already exists - skipping", e);
              return null;
            } else {
              throw e;
            }
          }
        });
  }

  @Override
  public String createGroup(String accessToken, String groupName) throws InterruptedException {
    SamRetry.retry(configurationService, () -> createGroupInner(accessToken, groupName));
    return SamRetry.retry(configurationService, () -> getGroupEmail(accessToken, groupName));
  }

  private void createGroupInner(String accessToken, String groupName) throws ApiException {
    samApiService.groupApi(accessToken).postGroup(groupName, null);
  }

  private String getGroupEmail(String accessToken, String groupName) throws ApiException {
    return samApiService.groupApi(accessToken).getGroup(groupName);
  }

  @Override
  public void overwriteGroupPolicyEmails(
      String accessToken, String groupName, String policyName, List<String> emailAddresses)
      throws InterruptedException {
    SamRetry.retry(
        configurationService,
        () -> overwriteGroupPolicyEmailsInner(accessToken, groupName, policyName, emailAddresses));
  }

  private void overwriteGroupPolicyEmailsInner(
      String accessToken, String groupName, String policyName, List<String> emailAddresses)
      throws ApiException {
    samApiService
        .groupApi(accessToken)
        .overwriteGroupPolicyEmails(groupName, policyName, emailAddresses);
  }

  @Override
  public void deleteGroup(String accessToken, String groupName) throws InterruptedException {
    SamRetry.retry(configurationService, () -> deleteGroupInner(accessToken, groupName));
  }

  private void deleteGroupInner(String accessToken, String groupName) throws ApiException {
    samApiService.groupApi(accessToken).deleteGroup(groupName);
  }

  @Override
  public String getPetToken(AuthenticatedUserRequest userReq, List<String> scopes)
      throws InterruptedException {
    return SamRetry.retry(configurationService, () -> getPetTokenInner(userReq, scopes));
  }

  private String getPetTokenInner(AuthenticatedUserRequest userReq, List<String> scopes)
      throws ApiException {
    return samApiService.googleApi(userReq.getToken()).getArbitraryPetServiceAccountToken(scopes);
  }

  @Override
  public String signUrlForBlob(
      AuthenticatedUserRequest userReq, String project, String path, Duration duration)
      throws InterruptedException {
    return SamRetry.retry(
        configurationService, () -> signUrlForBlobInner(userReq, project, path, duration));
  }

  private String signUrlForBlobInner(
      AuthenticatedUserRequest userReq, String project, String path, Duration duration)
      throws ApiException {
    RequesterPaysSignedUrlRequest request =
        new RequesterPaysSignedUrlRequest()
            .gsPath(path)
            .duration(BigDecimal.valueOf(duration.toMinutes()))
            .requesterPaysProject(project);
    return samApiService.googleApi(userReq.getToken()).getRequesterPaysSignedUrlForBlob(request);
  }

  public void azureCreateManagedResourceGroup(
      AuthenticatedUserRequest userReq,
      String billingProfileId,
      ManagedResourceGroupCoordinates managedResourceGroupCoordinates)
      throws ApiException {
    samApiService
        .azureApi(userReq.getToken())
        .createManagedResourceGroup(billingProfileId, managedResourceGroupCoordinates);
  }

  private UserStatusInfo getUserInfoAndVerify(AuthenticatedUserRequest userReq) {
    UserStatusInfo userStatusInfo = getUserInfo(userReq);
    if (!userStatusInfo.isEnabled()) {
      throw new IamForbiddenException(
          String.format("User %s is not enabled in Terra", userStatusInfo.getUserEmail()));
    }
    return userStatusInfo;
  }

  AccessPolicyMembershipRequest createAccessPolicyOne(IamRole role, String email) {
    return createAccessPolicy(role, List.of(email));
  }

  AccessPolicyMembershipRequest createAccessPolicy(IamRole role, List<String> emails) {
    AccessPolicyMembershipRequest membership =
        new AccessPolicyMembershipRequest().roles(List.of(role.toString()));
    if (emails != null) {
      membership.memberEmails(emails);
    }
    return membership;
  }

  /**
   * Syncing a policy with SAM results in a Google group being created that is tied to that policy.
   * The response is an object with one key that is the policy group email and a value that is a
   * list of objects.
   *
   * @param syncPolicyResponse map with one key that is an email
   * @return the policy group email
   */
  private String getPolicyGroupEmailFromResponse(
      Map<String, List<SyncReportEntry>> syncPolicyResponse) {
    if (syncPolicyResponse.size() != 1) {
      throw new IllegalArgumentException(
          "Expecting syncPolicyResponse to be an object with one key");
    }
    return syncPolicyResponse.keySet().iterator().next();
  }

  /**
   * Converts a SAM-specific ApiException to a DataRepo-specific common exception, based on the HTTP
   * status code.
   */
  public static ErrorReportException convertSAMExToDataRepoEx(final ApiException samEx) {
    logger.warn("SAM client exception code: {}", samEx.getCode());
    logger.warn("SAM client exception message: {}", samEx.getMessage());
    logger.warn("SAM client exception details: {}", samEx.getResponseBody());

    // Sometimes the sam message is buried several levels down inside of the error report object.
    String message = null;
    try {
      ErrorReport errorReport = objectMapper.readValue(samEx.getResponseBody(), ErrorReport.class);
      message = extractErrorMessage(errorReport);
    } catch (JsonProcessingException | IllegalArgumentException ex) {
      message = Objects.requireNonNullElse(samEx.getMessage(), "SAM client exception");
    }

    switch (samEx.getCode()) {
      case HttpStatusCodes.STATUS_CODE_BAD_REQUEST:
        {
          return new IamBadRequestException(message, samEx);
        }
      case HttpStatusCodes.STATUS_CODE_UNAUTHORIZED:
        {
          return new IamUnauthorizedException(message, samEx);
        }
      case HttpStatusCodes.STATUS_CODE_FORBIDDEN:
        {
          return new IamForbiddenException(message, samEx);
        }
      case HttpStatusCodes.STATUS_CODE_NOT_FOUND:
        {
          return new IamNotFoundException(message, samEx);
        }
      case HttpStatusCodes.STATUS_CODE_CONFLICT:
        {
          return new IamConflictException(message, samEx);
        }
        // SAM does not use a 501 NOT_IMPLEMENTED status code, so that case is skipped here
        // A 401 error will only occur when OpenDJ is down and should be raised as a 500 error
      default:
        {
          return new IamInternalServerErrorException(message, samEx);
        }
    }
  }

  @VisibleForTesting
  static String extractErrorMessage(final ErrorReport errorReport) {
    List<String> causes = new ArrayList<>();
    for (ErrorReport cause : errorReport.getCauses()) {
      causes.add(extractErrorMessage(cause));
    }

    String separator = (causes.isEmpty() ? "" : ": ");
    String openParen = (causes.size() > 1 ? "(" : "");
    String closeParen = (causes.size() > 1 ? ")" : "");
    return errorReport.getMessage()
        + separator
        + openParen
        + String.join(", ", causes)
        + closeParen;
  }
}
