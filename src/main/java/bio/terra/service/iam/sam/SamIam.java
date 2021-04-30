package bio.terra.service.iam.sam;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.ValidationUtils;
import bio.terra.common.exception.DataRepoException;
import bio.terra.model.PolicyModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.iam.exception.IamBadRequestException;
import bio.terra.service.iam.exception.IamConflictException;
import bio.terra.service.iam.exception.IamForbiddenException;
import bio.terra.service.iam.exception.IamInternalServerErrorException;
import bio.terra.service.iam.exception.IamNotFoundException;
import bio.terra.service.iam.exception.IamUnauthorizedException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpStatusCodes;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.Pair;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembership;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntry;
import org.broadinstitute.dsde.workbench.client.sam.model.ErrorReport;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component("iamProvider")
// Use @Profile to select when there is more than one IamService
public class SamIam implements IamProviderInterface {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final SamConfiguration samConfig;
    private final ConfigurationService configurationService;

    @Autowired
    public SamIam(SamConfiguration samConfig,
                  ConfigurationService configurationService) {
        this.samConfig = samConfig;
        this.configurationService = configurationService;
    }

    private static final Logger logger = LoggerFactory.getLogger(SamIam.class);

    private ApiClient getApiClient(String accessToken) {
        ApiClient apiClient = new ApiClient();
        apiClient.setAccessToken(accessToken);
        apiClient.setUserAgent("OpenAPI-Generator/1.0.0 java");  // only logs an error in sam
        return apiClient.setBasePath(samConfig.getBasePath());
    }

    private ApiClient getUnauthApiClient() {
        ApiClient apiClient = new ApiClient();
        apiClient.setUserAgent("OpenAPI-Generator/1.0.0 java");  // only logs an error in sam
        apiClient.setBasePath(samConfig.getBasePath());
        return apiClient;
    }

    @VisibleForTesting
    ResourcesApi samResourcesApi(String accessToken) {
        return new ResourcesApi(getApiClient(accessToken));
    }

    @VisibleForTesting
    StatusApi samStatusApi() {
        return new StatusApi(getUnauthApiClient());
    }

    @VisibleForTesting
    GoogleApi samGoogleApi(String accessToken) {
        return new GoogleApi(getApiClient(accessToken));
    }

    @VisibleForTesting
    UsersApi samUsersApi(String accessToken) {
        return new UsersApi(getApiClient(accessToken));
    }

    /**
     * Asks SAM if a user can do an action on a resource.
     * This method converts the SAM-specific ApiException to a data repo-specific common exception.
     *
     * @return true if authorized, false otherwise
     */
    @Override
    public boolean isAuthorized(AuthenticatedUserRequest userReq,
                                IamResourceType iamResourceType,
                                String resourceId,
                                IamAction action) throws InterruptedException {

        return SamRetry.retry(configurationService,
            () -> isAuthorizedInner(userReq, iamResourceType, resourceId, action));
    }

    private boolean isAuthorizedInner(AuthenticatedUserRequest userReq,
                                      IamResourceType iamResourceType,
                                      String resourceId,
                                      IamAction action) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        boolean authorized =
            samResourceApi.resourcePermissionV2(iamResourceType.toString(), resourceId, action.toString());
        logger.debug("authorized is " + authorized);
        return authorized;
    }

    @Override
    public List<UUID> listAuthorizedResources(AuthenticatedUserRequest userReq,
                                              IamResourceType iamResourceType) throws InterruptedException {
        return SamRetry.retry(configurationService,
            () -> listAuthorizedResourcesInner(userReq, iamResourceType));
    }

    private List<UUID> listAuthorizedResourcesInner(AuthenticatedUserRequest userReq,
                                                    IamResourceType iamResourceType) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());

        try (Stream<ResourceAndAccessPolicy> resultStream =
                 samResourceApi.listResourcesAndPolicies(iamResourceType.toString()).stream()) {
            return resultStream
                .map(ResourceAndAccessPolicy::getResourceId)
                // Convert valid UUID's to Optional<UUID> objects
                .map(ValidationUtils::convertToUuid)
                // Only return valid values
                .filter(Optional::isPresent)
                // Unbox the UUID
                .map(Optional::get)
                .collect(Collectors.toList());
        }
    }

    @Override
    public boolean hasActions(AuthenticatedUserRequest userReq,
                              IamResourceType iamResourceType,
                              String resourceId) throws InterruptedException {
        return SamRetry.retry(configurationService,
            () -> hasActionsInner(userReq, iamResourceType, resourceId));
    }

    private boolean hasActionsInner(AuthenticatedUserRequest userReq,
                                    IamResourceType iamResourceType,
                                    String resourceId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        List<String> actionList = samResourceApi.resourceActions(iamResourceType.toString(), resourceId);
        return (actionList.size() > 0);
    }

    @Override
    public void deleteDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId) throws InterruptedException {
        deleteResource(userReq, IamResourceType.DATASET, datasetId.toString());
    }

    @Override
    public void deleteSnapshotResource(AuthenticatedUserRequest userReq, UUID snapshotId) throws InterruptedException {
        deleteResource(userReq, IamResourceType.DATASNAPSHOT, snapshotId.toString());
    }

    private void deleteResource(AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId)
        throws InterruptedException {
        SamRetry.retry(configurationService,
            () -> deleteResourceInner(userReq, iamResourceType, resourceId));
    }

    private void deleteResourceInner(AuthenticatedUserRequest userReq,
                                     IamResourceType iamResourceType,
                                     String resourceId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        samResourceApi.deleteResource(iamResourceType.toString(), resourceId);
    }

    @Override
    public Map<IamRole, String> createDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId)
        throws InterruptedException {
        SamRetry.retry(configurationService,
            () -> createDatasetResourceInner(userReq, datasetId));
        return SamRetry.retry(configurationService,
            () -> syncDatasetResourcePoliciesInner(userReq, datasetId));
    }

    private void createDatasetResourceInner(AuthenticatedUserRequest userReq,
                                                            UUID datasetId) throws ApiException {
        CreateResourceCorrectRequest req = new CreateResourceCorrectRequest();
        req.setResourceId(datasetId.toString());
        req.addPoliciesItem(
            IamRole.ADMIN.toString(),
            createAccessPolicyOne(IamRole.ADMIN, samConfig.getAdminsGroupEmail()));
        req.addPoliciesItem(
            IamRole.STEWARD.toString(),
            createAccessPolicyOne(IamRole.STEWARD, userReq.getEmail()));
        req.addPoliciesItem(
            IamRole.CUSTODIAN.toString(),
            createAccessPolicyOne(IamRole.CUSTODIAN, userReq.getEmail()));
        req.addPoliciesItem(
            IamRole.SNAPSHOT_CREATOR.toString(),
            createAccessPolicy(IamRole.SNAPSHOT_CREATOR, null));

        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        logger.debug(req.toString());

        // create the resource in sam
        createResourceCorrectCall(samResourceApi.getApiClient(), IamResourceType.DATASET.toString(), req);
    }

    private Map<IamRole, String> syncDatasetResourcePoliciesInner(AuthenticatedUserRequest userReq,
                                                            UUID datasetId) throws ApiException {
        // This includes multiple calls to SAM within one retry call
        // retrySyncDatasetPolicies() integration test proves that we can re-run all of these calls
        // if the call fails part of the way through

        // we'll want all of these roles to have read access to the underlying data,
        // so we sync and return the emails for the policies that get created by SAM
        // Note: ADMIN explicitly does NOT require this since it does not require read access to the data
        Map<IamRole, String> policies = new HashMap<>();
        for (IamRole role : Arrays.asList(IamRole.STEWARD, IamRole.CUSTODIAN, IamRole.SNAPSHOT_CREATOR)) {
            String policy = syncOnePolicy(userReq, IamResourceType.DATASET, datasetId, role);
            policies.put(role, policy);
        }

        return policies;
    }

    @Override
    public Map<IamRole, String> createSnapshotResource(
        AuthenticatedUserRequest userReq,
        UUID snapshotId,
        List<String> readersList) throws InterruptedException {
        SamRetry.retry(configurationService,
            () -> createSnapshotResourceInner(userReq, snapshotId, readersList));
        return SamRetry.retry(configurationService,
            () -> syncSnapshotResourcePoliciesInner(userReq, snapshotId, readersList));
    }

    private void createSnapshotResourceInner(AuthenticatedUserRequest userReq,
                                                             UUID snapshotId,
                                                             List<String> readersList) throws ApiException {
        CreateResourceCorrectRequest req = new CreateResourceCorrectRequest();
        req.setResourceId(snapshotId.toString());
        req.addPoliciesItem(
            IamRole.ADMIN.toString(),
            createAccessPolicyOne(IamRole.ADMIN, samConfig.getAdminsGroupEmail()));
        req.addPoliciesItem(
            IamRole.STEWARD.toString(),
            createAccessPolicyOne(IamRole.STEWARD, userReq.getEmail()));
        req.addPoliciesItem(
            IamRole.READER.toString(),
            createAccessPolicy(IamRole.READER, readersList));
        req.addPoliciesItem(
            IamRole.DISCOVERER.toString(),
            createAccessPolicy(IamRole.DISCOVERER, null));

        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        logger.debug("SAM request: " + req.toString());

        // create the resource in sam
        createResourceCorrectCall(samResourceApi.getApiClient(), IamResourceType.DATASNAPSHOT.toString(), req);
    }

    private Map<IamRole, String> syncSnapshotResourcePoliciesInner(AuthenticatedUserRequest userReq,
                                                             UUID snapshotId,
                                                             List<String> readersList) throws ApiException {
        // sync the policies for all roles that have read data action
        Map<IamRole, String> policies = new HashMap<>();
        String policy = syncOnePolicy(userReq, IamResourceType.DATASNAPSHOT, snapshotId, IamRole.READER);
        policies.put(IamRole.READER, policy);
        policy = syncOnePolicy(userReq, IamResourceType.DATASNAPSHOT, snapshotId, IamRole.STEWARD);
        policies.put(IamRole.STEWARD, policy);
        return policies;
    }

    private String syncOnePolicy(AuthenticatedUserRequest userReq,
                                 IamResourceType resourceType,
                                 UUID id,
                                 IamRole role) throws ApiException {
        Map<String, List<Object>> results = samGoogleApi(userReq.getRequiredToken()).syncPolicy(
            resourceType.toString(),
            id.toString(),
            role.toString());
        String policyEmail = getPolicyGroupEmailFromResponse(results);
        logger.debug("Policy Group Resource: {} Role: {} Email:  {} ",
            resourceType.toString(), role.toString(), policyEmail);
        return policyEmail;
    }

    @Override
    public void createProfileResource(AuthenticatedUserRequest userReq, String profileId) throws InterruptedException {
        SamRetry.retry(configurationService,
            () -> createProfileResourceInner(userReq, profileId));
    }

    private void createProfileResourceInner(AuthenticatedUserRequest userReq, String profileId) throws ApiException {
        CreateResourceCorrectRequest req = new CreateResourceCorrectRequest();
        req.setResourceId(profileId);
        req.addPoliciesItem(
            IamRole.ADMIN.toString(),
            createAccessPolicyOne(IamRole.ADMIN, samConfig.getAdminsGroupEmail()));
        req.addPoliciesItem(
            IamRole.OWNER.toString(),
            createAccessPolicyOne(IamRole.OWNER, userReq.getEmail()));
        req.addPoliciesItem(
            IamRole.USER.toString(),
            createAccessPolicy(IamRole.USER, null));

        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        logger.debug("SAM request: " + req.toString());

        // Simply ensure that the call can complete
        createResourceCorrectCall(samResourceApi.getApiClient(), IamResourceType.SPEND_PROFILE.toString(), req);
    }

    @Override
    public void deleteProfileResource(AuthenticatedUserRequest userReq, String profileId)
        throws InterruptedException {
        deleteResource(userReq, IamResourceType.SPEND_PROFILE, profileId.toString());
    }

    @Override
    public List<PolicyModel> retrievePolicies(AuthenticatedUserRequest userReq,
                                              IamResourceType iamResourceType,
                                              UUID resourceId) throws InterruptedException {
        return SamRetry.retry(configurationService,
            () -> retrievePoliciesInner(userReq, iamResourceType, resourceId));
    }

    private List<PolicyModel> retrievePoliciesInner(AuthenticatedUserRequest userReq,
                                                    IamResourceType iamResourceType,
                                                    UUID resourceId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        try (Stream<AccessPolicyResponseEntry> resultStream =
                 samResourceApi.listResourcePolicies(iamResourceType.toString(), resourceId.toString()).stream()) {
            return resultStream.map(entry -> new PolicyModel()
                .name(entry.getPolicyName())
                .members(entry.getPolicy().getMemberEmails()))
                .collect(Collectors.toList());
        }
    }

    @Override
    public Map<IamRole, String> retrievePolicyEmails(AuthenticatedUserRequest userReq,
                                                     IamResourceType iamResourceType,
                                                     UUID resourceId) throws InterruptedException {
        return SamRetry.retry(configurationService,
            () -> retrievePolicyEmailsInner(userReq, iamResourceType, resourceId));
    }

    private Map<IamRole, String> retrievePolicyEmailsInner(AuthenticatedUserRequest userReq,
                                                           IamResourceType iamResourceType,
                                                           UUID resourceId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        try (Stream<AccessPolicyResponseEntry> resultStream =
                 samResourceApi.listResourcePolicies(iamResourceType.toString(), resourceId.toString()).stream()) {
            return resultStream
                .collect(Collectors.toMap(
                    a -> IamRole.fromValue(a.getPolicyName()),
                    AccessPolicyResponseEntry::getEmail
                ));
        }
    }

    @Override
    public PolicyModel addPolicyMember(AuthenticatedUserRequest userReq,
                                       IamResourceType iamResourceType,
                                       UUID resourceId,
                                       String policyName,
                                       String userEmail) throws InterruptedException {
        SamRetry.retry(configurationService,
            () -> addPolicyMemberInner(userReq, iamResourceType, resourceId, policyName, userEmail));
        return SamRetry.retry(configurationService,
            () -> retrievePolicy(userReq, iamResourceType, resourceId, policyName));
    }

    private void addPolicyMemberInner(AuthenticatedUserRequest userReq,
                                             IamResourceType iamResourceType,
                                             UUID resourceId,
                                             String policyName,
                                             String userEmail) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        logger.debug("addUserPolicy resourceType {} resourceId {} policyName {} userEmail {}",
            iamResourceType.toString(), resourceId.toString(), policyName, userEmail);
        samResourceApi.addUserToPolicy(iamResourceType.toString(), resourceId.toString(), policyName, userEmail);
    }

    @Override
    public PolicyModel deletePolicyMember(AuthenticatedUserRequest userReq,
                                          IamResourceType iamResourceType,
                                          UUID resourceId,
                                          String policyName,
                                          String userEmail) throws InterruptedException {
        SamRetry.retry(configurationService,
            () -> deletePolicyMemberInner(userReq, iamResourceType, resourceId, policyName, userEmail));
        return SamRetry.retry(configurationService,
            () -> retrievePolicy(userReq, iamResourceType, resourceId, policyName));
    }

    private void deletePolicyMemberInner(AuthenticatedUserRequest userReq,
                                                IamResourceType iamResourceType,
                                                UUID resourceId,
                                                String policyName,
                                                String userEmail) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        samResourceApi.removeUserFromPolicy(
            iamResourceType.toString(),
            resourceId.toString(),
            policyName,
            userEmail);
    }

    private PolicyModel retrievePolicy(AuthenticatedUserRequest userReq,
                                       IamResourceType iamResourceType,
                                       UUID resourceId,
                                       String policyName) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        AccessPolicyMembership result =
            samResourceApi.getPolicy(iamResourceType.toString(), resourceId.toString(), policyName);
        return new PolicyModel()
            .name(policyName)
            .members(result.getMemberEmails());
    }

    @Override
    public UserStatusInfo getUserInfo(AuthenticatedUserRequest userReq) {
        UsersApi samUsersApi = samUsersApi(userReq.getRequiredToken());
        try {
            org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo samInfo = samUsersApi.getUserStatusInfo();
            return new UserStatusInfo().userSubjectId(samInfo.getUserSubjectId())
                .userEmail(samInfo.getUserEmail())
                .enabled(samInfo.getEnabled());
        } catch (ApiException ex) {
            throw convertSAMExToDataRepoEx(ex);
        }
    }

    AccessPolicyMembership createAccessPolicyOne(IamRole role, String email) {
        return createAccessPolicy(role, Collections.singletonList(email));
    }

    AccessPolicyMembership createAccessPolicy(IamRole role, List<String> emails) {
        AccessPolicyMembership membership = new AccessPolicyMembership()
            .roles(Collections.singletonList(role.toString()));
        if (emails != null) {
            membership.memberEmails(emails);
        }
        return membership;
    }

    // This is a work around for https://broadworkbench.atlassian.net/browse/AP-149
    // This is a copy of the ApiClient.createResourceCall but adds in the validation and
    // the actual execution of the call. And doesn't allow listener callbacks
    private void createResourceCorrectCall(
        ApiClient localVarApiClient,
        String resourceTypeName,
        CreateResourceCorrectRequest resourceCreate) throws ApiException {

        // verify the required parameter 'resourceTypeName' is set
        if (resourceTypeName == null) {
            throw new ApiException(
                "Missing the required parameter 'resourceTypeName' when calling createResource(Async)");
        }

        // verify the required parameter 'resourceCreate' is set
        if (resourceCreate == null) {
            throw new ApiException(
                "Missing the required parameter 'resourceCreate' when calling createResource(Async)");
        }

        // create path and map variables
        String localVarPath = "/api/resources/v1/" + localVarApiClient.escapeString(resourceTypeName);

        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();
        final String[] localVarAccepts = {
            "application/json"
        };
        final String localVarAccept = localVarApiClient.selectHeaderAccept(localVarAccepts);
        if (localVarAccept != null) {
            localVarHeaderParams.put("Accept", localVarAccept);
        }

        final String[] localVarContentTypes = {
        };
        final String localVarContentType = localVarApiClient.selectHeaderContentType(localVarContentTypes);
        localVarHeaderParams.put("Content-Type", localVarContentType);


        String[] localVarAuthNames = new String[]{"googleoauth"};
        okhttp3.Call localVarCall = localVarApiClient.buildCall(
            localVarPath,
            "POST",
            localVarQueryParams,
            localVarCollectionQueryParams,
            resourceCreate,
            localVarHeaderParams,
            localVarFormParams,
            localVarAuthNames,
            null);
        localVarApiClient.execute(localVarCall);
    }

    /**
     * Syncing a policy with SAM results in a Google group being created that is tied to that policy. The response is an
     * object with one key that is the policy group email and a value that is a list of objects.
     *
     * @param syncPolicyResponse map with one key that is an email
     * @return the policy group email
     */
    private String getPolicyGroupEmailFromResponse(Map<String, List<Object>> syncPolicyResponse) {
        if (syncPolicyResponse.size() != 1) {
            throw new IllegalArgumentException("Expecting syncPolicyResponse to be an object with one key");
        }
        return syncPolicyResponse.keySet().iterator().next();
    }

    /**
     * Converts a SAM-specific ApiException to a DataRepo-specific common exception, based on the HTTP status code.
     */
    public static DataRepoException convertSAMExToDataRepoEx(final ApiException samEx) {
        logger.warn("SAM client exception code: {}", samEx.getCode());
        logger.warn("SAM client exception message: {}", samEx.getMessage());
        logger.warn("SAM client exception details: {}", samEx.getResponseBody());

        // Sometimes the sam message is buried several levels down inside of the error report object.
        // If we find an empty message then we try to deserialize the error report and use that message.
        String message = samEx.getMessage();
        if (StringUtils.isEmpty(message)) {
            try {
                ErrorReport errorReport = objectMapper.readValue(samEx.getResponseBody(), ErrorReport.class);
                message = extractErrorMessage(errorReport);
            } catch (JsonProcessingException ex) {
                logger.debug("Unable to deserialize sam exception response body");
            }
        }

        switch (samEx.getCode()) {
            case HttpStatusCodes.STATUS_CODE_BAD_REQUEST: {
                return new IamBadRequestException(message, samEx);
            }
            case HttpStatusCodes.STATUS_CODE_UNAUTHORIZED: {
                return new IamUnauthorizedException(message, samEx);
            }
            case HttpStatusCodes.STATUS_CODE_FORBIDDEN: {
                return new IamForbiddenException(message, samEx);
            }
            case HttpStatusCodes.STATUS_CODE_NOT_FOUND: {
                return new IamNotFoundException(message, samEx);
            }
            case HttpStatusCodes.STATUS_CODE_CONFLICT: {
                return new IamConflictException(message, samEx);
            }
            case HttpStatusCodes.STATUS_CODE_SERVER_ERROR: {
                return new IamInternalServerErrorException(message, samEx);
            }
            // note that SAM does not use a 501 NOT_IMPLEMENTED status code, so that case is skipped here
            default: {
                return new IamInternalServerErrorException(message, samEx);
            }
        }
    }

    @Override
    public RepositoryStatusModelSystems samStatus() {
        try {
            return SamRetry.retry(configurationService, () -> {
                StatusApi samApi = samStatusApi();
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
                .message(errorMsg + ": " + ex.toString());
        }
    }

    @VisibleForTesting
    static String extractErrorMessage(final ErrorReport errorReport) {
        List<String> causes = new ArrayList<>();
        for (ErrorReport cause: errorReport.getCauses()) {
            causes.add(extractErrorMessage(cause));
        }

        String separator = (causes.isEmpty() ? "" : ": ");
        String openParen = (causes.size() > 1 ? "(" : "");
        String closeParen = (causes.size() > 1 ? ")" : "");
        return errorReport.getMessage() + separator +
            openParen + String.join(", ", causes) + closeParen;
    }

}
