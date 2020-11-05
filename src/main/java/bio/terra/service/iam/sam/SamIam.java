package bio.terra.service.iam.sam;

import bio.terra.common.exception.DataRepoException;
import bio.terra.model.PolicyModel;
import bio.terra.model.UserStatusInfo;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.iam.exception.IamBadRequestException;
import bio.terra.service.iam.exception.IamInternalServerErrorException;
import bio.terra.service.iam.exception.IamNotFoundException;
import bio.terra.service.iam.exception.IamUnauthorizedException;
import com.google.api.client.http.HttpStatusCodes;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.Pair;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembership;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntry;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
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
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component("iamProvider")
// Use @Profile to select when there is more than one IamService
public class SamIam implements IamProviderInterface {
    private final SamConfiguration samConfig;
    private final ConfigurationService configurationService;

    @Autowired
    public SamIam(SamConfiguration samConfig, ConfigurationService configurationService) {
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

    private ResourcesApi samResourcesApi(String accessToken) {
        return new ResourcesApi(getApiClient(accessToken));
    }

    private GoogleApi samGoogleApi(String accessToken) {
        return new GoogleApi(getApiClient(accessToken));
    }

    private UsersApi samUsersApi(String accessToken) {
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

        SamRetry samRetry = new SamRetry(configurationService);
        return samRetry.perform(() -> isAuthorizedInner(userReq, iamResourceType, resourceId, action));
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
        SamRetry samRetry = new SamRetry(configurationService);
        return samRetry.perform(() -> listAuthorizedResourcesInner(userReq, iamResourceType));
    }

    private List<UUID> listAuthorizedResourcesInner(AuthenticatedUserRequest userReq,
                                                    IamResourceType iamResourceType) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());

        try (Stream<ResourceAndAccessPolicy> resultStream =
                 samResourceApi.listResourcesAndPolicies(iamResourceType.toString()).stream()) {
            return resultStream
                .map(resource -> UUID.fromString(resource.getResourceId()))
                .collect(Collectors.toList());
        }
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

        SamRetry samRetry = new SamRetry(configurationService);
        samRetry.perform(() -> deleteResourceInner(userReq, iamResourceType, resourceId));
    }

    // Return useless boolean to match the SamFunction signature for retry
    private boolean deleteResourceInner(AuthenticatedUserRequest userReq,
                                        IamResourceType iamResourceType,
                                        String resourceId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        samResourceApi.deleteResource(iamResourceType.toString(), resourceId);
        return true;
    }

    @Override
    public Map<IamRole, String> createDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId)
        throws InterruptedException {
        SamRetry samRetry = new SamRetry(configurationService);
        return samRetry.perform(() -> createDatasetResourceInner(userReq, datasetId));
    }

    private Map<IamRole, String> createDatasetResourceInner(AuthenticatedUserRequest userReq,
                                                            UUID datasetId) throws ApiException {
        CreateResourceCorrectRequest req = new CreateResourceCorrectRequest();
        req.setResourceId(datasetId.toString());
        req.addPoliciesItem(
            IamRole.STEWARD.toString(),
            createAccessPolicyOne(IamRole.STEWARD, samConfig.getStewardsGroupEmail()));
        req.addPoliciesItem(
            IamRole.CUSTODIAN.toString(),
            createAccessPolicyOne(IamRole.CUSTODIAN, userReq.getEmail()));
        req.addPoliciesItem(
            IamRole.INGESTER.toString(),
            createAccessPolicy(IamRole.INGESTER, null));

        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        logger.debug(req.toString());

        // create the resource in sam
        createResourceCorrectCall(samResourceApi.getApiClient(), IamResourceType.DATASET.toString(), req);

        // we'll want all of these roles to have read access to the underlying data,
        // so we sync and return the emails for the policies that get created by SAM
        Map<IamRole, String> policies = new HashMap<>();
        for (IamRole role : Arrays.asList(IamRole.STEWARD, IamRole.CUSTODIAN, IamRole.INGESTER)) {
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

        SamRetry samRetry = new SamRetry(configurationService);
        return samRetry.perform(() -> createSnapshotResourceInner(userReq, snapshotId, readersList));
    }

    private Map<IamRole, String> createSnapshotResourceInner(AuthenticatedUserRequest userReq,
                                                             UUID snapshotId,
                                                             List<String> readersList) throws ApiException {
        CreateResourceCorrectRequest req = new CreateResourceCorrectRequest();
        req.setResourceId(snapshotId.toString());
        req.addPoliciesItem(
            IamRole.STEWARD.toString(),
            createAccessPolicyOne(IamRole.STEWARD, samConfig.getStewardsGroupEmail()));
        req.addPoliciesItem(
            IamRole.CUSTODIAN.toString(),
            createAccessPolicyOne(IamRole.CUSTODIAN, userReq.getEmail()));
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

        // sync the policies for all roles that have read data action
        Map<IamRole, String> policies = new HashMap<>();
        String policy = syncOnePolicy(userReq, IamResourceType.DATASNAPSHOT, snapshotId, IamRole.READER);
        policies.put(IamRole.READER, policy);
        policy = syncOnePolicy(userReq, IamResourceType.DATASNAPSHOT, snapshotId, IamRole.CUSTODIAN);
        policies.put(IamRole.CUSTODIAN, policy);
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
    public List<PolicyModel> retrievePolicies(AuthenticatedUserRequest userReq,
                                              IamResourceType iamResourceType,
                                              UUID resourceId) throws InterruptedException {
        SamRetry samRetry = new SamRetry(configurationService);
        return samRetry.perform(() -> retrievePoliciesInner(userReq, iamResourceType, resourceId));
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
        SamRetry samRetry = new SamRetry(configurationService);
        return samRetry.perform(() -> retrievePolicyEmailsInner(userReq, iamResourceType, resourceId));
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
        SamRetry samRetry = new SamRetry(configurationService);
        return samRetry.perform(
            () -> addPolicyMemberInner(userReq, iamResourceType, resourceId, policyName, userEmail));
    }

    private PolicyModel addPolicyMemberInner(AuthenticatedUserRequest userReq,
                                             IamResourceType iamResourceType,
                                             UUID resourceId,
                                             String policyName,
                                             String userEmail) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        samResourceApi.addUserToPolicy(iamResourceType.toString(), resourceId.toString(), policyName, userEmail);

        AccessPolicyMembership result =
            samResourceApi.getPolicy(iamResourceType.toString(), resourceId.toString(), policyName);
        return new PolicyModel()
            .name(policyName)
            .members(result.getMemberEmails());
    }

    @Override
    public PolicyModel deletePolicyMember(AuthenticatedUserRequest userReq,
                                          IamResourceType iamResourceType,
                                          UUID resourceId,
                                          String policyName,
                                          String userEmail) throws InterruptedException {
        SamRetry samRetry = new SamRetry(configurationService);
        return samRetry.perform(
            () -> deletePolicyMemberInner(userReq, iamResourceType, resourceId, policyName, userEmail));
    }

    private PolicyModel deletePolicyMemberInner(AuthenticatedUserRequest userReq,
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
        // TODO: add mapping based on HTTP status code
        // SAM uses com.google.api.client.http.HttpStatusCodes
        // DataRepo uses org.springframework.http.HttpStatus

        logger.warn("SAM client exception code: {}", samEx.getCode());
        logger.warn("SAM client exception message: {}", samEx.getMessage());
        logger.warn("SAM client exception details: {}", samEx.getResponseBody());
        switch (samEx.getCode()) {
            case HttpStatusCodes.STATUS_CODE_BAD_REQUEST: {
                return new IamBadRequestException(samEx);
            }
            case HttpStatusCodes.STATUS_CODE_UNAUTHORIZED: {
                return new IamUnauthorizedException(samEx);
            }
            case HttpStatusCodes.STATUS_CODE_NOT_FOUND: {
                return new IamNotFoundException(samEx);
            }
            case HttpStatusCodes.STATUS_CODE_SERVER_ERROR: {
                return new IamInternalServerErrorException(samEx);
            }
            // note that SAM does not use a 501 NOT_IMPLEMENTED status code, so that case is skipped here
            default: {
                return new IamInternalServerErrorException(samEx);
            }
        }
    }

    @Override
    public RepositoryStatusModelSystems samStatus() {
        SamRetry samRetry = new SamRetry(configurationService);
        try {
            return samRetry.perform(() -> {
                StatusApi samApi = new StatusApi(getUnauthApiClient());
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

}
